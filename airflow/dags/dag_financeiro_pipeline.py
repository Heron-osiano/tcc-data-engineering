"""
dag_financeiro_pipeline.py
==========================
Pipeline financeiro — PostgreSQL → Silver Iceberg

  1. silver_custos_produtos (Glue)
  2. silver_impostos (Glue)

Roda todo dia às 05:00 UTC.
As duas tasks rodam em paralelo pois são independentes.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner":            "tcc",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="financeiro_pipeline",
    description="Pipeline financeiro PostgreSQL → Silver Iceberg",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval="0 5 * * *",   # todo dia às 05:00 UTC
    catchup=False,
    tags=["tcc", "financeiro", "silver"],
) as dag:

    task_custos = GlueJobOperator(
        task_id="silver_custos_produtos",
        job_name="silver_custos_produtos",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    task_impostos = GlueJobOperator(
        task_id="silver_impostos",
        job_name="silver_impostos",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    # Rodam em paralelo — são independentes entre si
    [task_custos, task_impostos]
