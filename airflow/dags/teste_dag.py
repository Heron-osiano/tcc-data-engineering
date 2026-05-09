from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="teste_ok",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    inicio = EmptyOperator(task_id="inicio")
