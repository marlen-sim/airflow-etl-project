from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False)
def test_postgres_connection():
    @task
    def check_connection():
        hook = PostgresHook(postgres_conn_id="postgre-dev")
        result = hook.get_first("SELECT 1;")
        print(f"Connection OK, result: {result}")

    check_connection()

test_postgres_connection()
