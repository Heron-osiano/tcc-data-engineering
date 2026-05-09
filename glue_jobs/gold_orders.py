"""
gold_orders.py
==============
Glue Job — Silver → Gold (orders)

Lógica:
  1. Lê as tabelas Silver de orders, custos, impostos e shipments
  2. Faz os joins e calcula receita por linha
  3. Faz MERGE INTO na tabela Iceberg Gold pelo id + order_item_item_id
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------
# Configurações
# ------------------------------------------------------------------
BUCKET        = "tcc-uspesalq"
GOLD_PATH     = f"s3://{BUCKET}/gold/mercado_livre/orders/"
GLUE_DATABASE = "gold_mercado_livre"
GLUE_TABLE    = "orders"

# Configura o Glue Catalog como catálogo Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET}/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# ------------------------------------------------------------------
# Passo 1 — Executa a query Gold diretamente nas tabelas Silver
# ------------------------------------------------------------------
df_gold = spark.sql("""
    WITH base AS (
        SELECT
            A.id,
            A.status,
            A.date_created,
            A.date_closed,
            A.date_last_updated,
            A.pack_id,
            A.total_amount,
            A._ingested_at,
            A.shipping_id,
            A.buyer_id,
            A.buyer_nickname,
            A.payment_transaction_amount,
            A.payment_transaction_amount_refunded,
            A.payment_installments,
            A.payment_id,
            A.payment_date_last_modified,
            A.payment_date_created,
            A.payment_method_id,
            A.payment_type,
            A.payment_order_id,
            A.payment_status,
            A.order_item_quantity,
            A.order_item_unit_price,
            (A.order_item_quantity * A.order_item_unit_price) AS order_price,
            (A.order_item_quantity * A.order_item_sale_fee)   AS order_sale_fee,
            A.order_item_item_id,
            A.order_item_item_title,
            A.order_item_item_category_id,
            A.order_item_item_seller_sku,
            A.order_date,
            (A.order_item_quantity * B.preco) AS custo_produto,
            C.imposto                         AS percentual_imposto,
            (A.total_amount * C.imposto)      AS valor_imposto,
            D.cost                            AS custo_frete_total,
            SUM(A.total_amount) OVER (PARTITION BY A.shipping_id) AS total_amount_shipment
        FROM glue_catalog.silver_mercado_livre.orders A
        LEFT JOIN glue_catalog.silver_financeiro.custos_produtos B
            ON  A.order_item_item_seller_sku = B.sku
            AND YEAR(A.order_date)  = YEAR(B.vigencia)
            AND MONTH(A.order_date) = MONTH(B.vigencia)
        LEFT JOIN glue_catalog.silver_financeiro.impostos C
            ON  YEAR(A.order_date)  = YEAR(C.vigencia)
            AND MONTH(A.order_date) = MONTH(C.vigencia)
        LEFT JOIN glue_catalog.silver_mercado_livre.shipments D
            ON A.shipping_id = D.shipment_id
    ),
    base_2 AS (
        SELECT
            *,
            ROUND(total_amount / NULLIF(total_amount_shipment, 0), 4) AS percentual_shipment,
            ROUND(custo_frete_total * (total_amount / NULLIF(total_amount_shipment, 0)), 2) AS custo_frete_rateado
        FROM base
    )
    SELECT
        *,
        ROUND(
            order_price - order_sale_fee - custo_produto - valor_imposto - custo_frete_rateado,
        2) AS receita
    FROM base_2
""")

# ------------------------------------------------------------------
# Passo 2 — Cria tabela Iceberg Gold se não existir
# ------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GLUE_DATABASE}.{GLUE_TABLE} (
        id                              BIGINT,
        status                          STRING,
        date_created                    TIMESTAMP,
        date_closed                     TIMESTAMP,
        date_last_updated               TIMESTAMP,
        pack_id                         BIGINT,
        total_amount                    DOUBLE,
        _ingested_at                    TIMESTAMP,
        shipping_id                     BIGINT,
        buyer_id                        BIGINT,
        buyer_nickname                  STRING,
        payment_transaction_amount      DOUBLE,
        payment_transaction_amount_refunded DOUBLE,
        payment_installments            INT,
        payment_id                      BIGINT,
        payment_date_last_modified      TIMESTAMP,
        payment_date_created            TIMESTAMP,
        payment_method_id               STRING,
        payment_type                    STRING,
        payment_order_id                BIGINT,
        payment_status                  STRING,
        order_item_quantity             INT,
        order_item_unit_price           DOUBLE,
        order_price                     DOUBLE,
        order_sale_fee                  DOUBLE,
        order_item_item_id              STRING,
        order_item_item_title           STRING,
        order_item_item_category_id     STRING,
        order_item_item_seller_sku      STRING,
        order_date                      DATE,
        custo_produto                   DOUBLE,
        percentual_imposto              DOUBLE,
        valor_imposto                   DOUBLE,
        custo_frete_total               DOUBLE,
        total_amount_shipment           DOUBLE,
        percentual_shipment             DOUBLE,
        custo_frete_rateado             DOUBLE,
        receita                         DOUBLE
    )
    USING iceberg
    LOCATION '{GOLD_PATH}'
    TBLPROPERTIES (
        'table_type'                      = 'ICEBERG',
        'format'                          = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    PARTITIONED BY (order_date)
""")

# ------------------------------------------------------------------
# Passo 3 — MERGE INTO Gold
# Chave: id + order_item_item_id (uma linha por order/item)
# ------------------------------------------------------------------
df_gold.createOrReplaceTempView("gold_staging")

spark.sql(f"""
    MERGE INTO glue_catalog.{GLUE_DATABASE}.{GLUE_TABLE} AS target
    USING gold_staging AS source
        ON  target.id                 = source.id
        AND target.order_item_item_id = source.order_item_item_id
    WHEN MATCHED AND source.date_last_updated > target.date_last_updated THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

job.commit()