from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests

@dag(
    schedule="@daily",       # можно запускать раз в день
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "practice"],
)
def exchange_rate_etl():

    @task
    def create_table():
        """Убеждаемся, что таблица существует — тоже часть идемпотентности."""
        hook = PostgresHook(postgres_conn_id="postgre-dev")
        hook.run("""
            CREATE TABLE IF NOT EXISTS exchange_rates (
                base_currency TEXT NOT NULL,
                target_currency TEXT NOT NULL,
                rate NUMERIC NOT NULL,
                rate_date DATE NOT NULL,
                PRIMARY KEY (base_currency, target_currency, rate_date)
            );
        """)

    @task
    def extract():
        """Тянем сырые данные из публичного API."""
        response = requests.get("https://api.frankfurter.app/latest?from=USD")
        response.raise_for_status()
        return response.json()

    @task
    def transform(raw_data: dict):
        """Превращаем JSON в список простых строк для вставки."""
        base = raw_data["base"]
        date = raw_data["date"]
        rates = raw_data["rates"]

        rows = [
            {"base": base, "target": currency, "rate": rate, "date": date}
            for currency, rate in rates.items()
        ]
        return rows

    @task
    def load(rows: list[dict]):
        """Идемпотентная загрузка через upsert."""
        hook = PostgresHook(postgres_conn_id="postgre-dev")
        for row in rows:
            hook.run(
                """
                INSERT INTO exchange_rates (base_currency, target_currency, rate, rate_date)
                VALUES (%(base)s, %(target)s, %(rate)s, %(date)s)
                ON CONFLICT (base_currency, target_currency, rate_date)
                DO UPDATE SET rate = EXCLUDED.rate;
                """,
                parameters=row,
            )

    # Порядок задач: create_table должна быть готова до load,
    # extract -> transform -> load идёт по цепочке через return-значения
    table = create_table()
    raw = extract()
    clean = transform(raw)
    loaded = load(clean)

    table >> loaded  # явно говорим: таблица должна быть создана раньше загрузки

exchange_rate_etl()
