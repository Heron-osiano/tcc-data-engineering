"""
dag_meli_pipeline.py
====================
Pipeline completo do Mercado Livre:

  1. meli_orders_to_s3        → Bronze orders
  2. silver_orders (Glue)     → Silver orders
  3. meli_shipments_to_s3     → Bronze shipments
  4. silver_shipments (Glue)  → Silver shipments
  5. gold_orders (Glue)       → Gold orders

Cada etapa só roda se a anterior tiver sucesso.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# Importa as funções de ingestão
# Os scripts estão em /opt/airflow/ingestion (PYTHONPATH configurado no docker-compose)
from meli_orders_to_s3    import run as run_orders
from meli_shipments_to_s3 import run as run_shipments

# ------------------------------------------------------------------
# Configurações padrão
# ------------------------------------------------------------------
default_args = {
    "owner":            "tcc",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

# ------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------
with DAG(
    dag_id="meli_pipeline",
    description="Pipeline completo Mercado Livre → Bronze → Silver → Gold",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule_interval="0 6 * * *",   # roda todo dia às 6h (horário UTC)
    catchup=False,
    tags=["tcc", "mercadolivre", "pipeline"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1 — Ingestão de Orders → Bronze S3
    # ------------------------------------------------------------------
    task_orders_bronze = PythonOperator(
        task_id="meli_orders_to_s3",
        python_callable=run_orders,
    )

    # ------------------------------------------------------------------
    # Task 2 — Glue Job Silver Orders
    # ------------------------------------------------------------------
    task_silver_orders = GlueJobOperator(
        task_id="silver_orders",
        job_name="silver_orders",         # nome do job no AWS Glue
        aws_conn_id="aws_default",        # conexão AWS configurada no Airflow
        region_name="us-east-1",
        wait_for_completion=True,
    )

    # ------------------------------------------------------------------
    # Task 3 — Ingestão de Shipments → Bronze S3
    # ------------------------------------------------------------------
    task_shipments_bronze = PythonOperator(
        task_id="meli_shipments_to_s3",
        python_callable=run_shipments,
    )

    # ------------------------------------------------------------------
    # Task 4 — Glue Job Silver Shipments
    # ------------------------------------------------------------------
    task_silver_shipments = GlueJobOperator(
        task_id="silver_shipments",
        job_name="silver_shipments",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    # ------------------------------------------------------------------
    # Task 5 — Glue Job Gold Orders
    # ------------------------------------------------------------------
    task_gold_orders = GlueJobOperator(
        task_id="gold_orders",
        job_name="gold_orders",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
    )

    # ------------------------------------------------------------------
    # Dependências — cada task só roda se a anterior tiver sucesso
    # ------------------------------------------------------------------
    (
        task_orders_bronze
        >> task_silver_orders
        >> task_shipments_bronze
        >> task_silver_shipments
        >> task_gold_orders
    )
