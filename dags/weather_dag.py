"""
weather_dag.py
Ежедневный ETL: Open-Meteo → PostgreSQL + Parquet
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


# ── Параметры DAG ──────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,       # не ждёт успеха предыдущего запуска
    "retries": 1,                   # повтор при падении таска
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_etl_daily",
    default_args=default_args,
    description="Инкрементная загрузка погодных данных из Open-Meteo",
    schedule_interval="0 6 * * *",  # каждый день в 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,                  # не догонять пропущенные запуски
    tags=["weather", "etl", "open-meteo"],
) as dag:

    #  проверка соединения с БД
    def check_db_connection():
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from dotenv import load_dotenv
        load_dotenv()

        import psycopg2
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=int(os.environ["DB_PORT"]),
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        )
        conn.close()
        print("✅ Соединение с БД установлено")

    #  запуск ETL
    def run_etl(**context):
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
        from dotenv import load_dotenv
        load_dotenv()

        import etl_module
        df = etl_module.run_incremental_etl()

        loaded = df.height if not df.is_empty() else 0
        print(f"Загружено строк: {loaded}")

        # передаём результат следующему таску через XCom
        return loaded

    # ── Определение тасков ────────────────────────────────────
    task_check_db = PythonOperator(
        task_id="check_db_connection",
        python_callable=check_db_connection,
    )

    task_run_etl = PythonOperator(
        task_id="run_weather_etl",
        python_callable=run_etl,
    )

    # ── Порядок выполнения ────────────────────────────────────
    task_check_db >> task_run_etl