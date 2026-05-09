"""
silver_orders.py
================
Glue Job — Bronze → Silver (orders)

Lógica:
  1. Lê todos os JSONL da Bronze
  2. Mantém só o primeiro payment aprovado por order
  3. Explode order_items (uma linha por item)
  4. Seleciona e tipa as colunas definidas
  5. Deduplica por order_id + order_item_item.id (mais recente)
  6. Faz MERGE INTO na tabela Iceberg da Silver
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

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
SILVER_PATH   = f"s3://{BUCKET}/silver/mercado_livre/orders/"
GLUE_DATABASE = "silver_mercado_livre"
GLUE_TABLE    = "orders"

# Configura o Glue Catalog como catálogo Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET}/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# ------------------------------------------------------------------
# Passo 1 — Lê apenas a partição do dia de execução da Bronze
# ------------------------------------------------------------------
from datetime import datetime, timezone
import boto3

today       = datetime.now(timezone.utc)
BRONZE_PATH = (
    f"s3://{BUCKET}/bronze/mercado_livre/orders/"
    f"year={today.year}/"
    f"month={today.month:02d}/"
    f"day={today.day:02d}/"
)

# Verifica se existe algum arquivo na partição antes de tentar ler
s3     = boto3.client("s3")
prefix = (
    f"bronze/mercado_livre/orders/"
    f"year={today.year}/"
    f"month={today.month:02d}/"
    f"day={today.day:02d}/"
)
response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)

if response.get("KeyCount", 0) == 0:
    print(f"Nenhum arquivo encontrado em {BRONZE_PATH} — nada a processar.")
    job.commit()
    sys.exit(0)

df = spark.read.json(BRONZE_PATH)

# ------------------------------------------------------------------
# Passo 2 — Mantém só o primeiro payment aprovado
# Igual ao script Python:
#   pagamentos_aprovados = [p for p in payments if p["status"] == "approved"]
#   order["payment"] = pagamentos_aprovados[0] if pagamentos_aprovados else None
# ------------------------------------------------------------------
df = df.withColumn(
    "payment",
    F.filter(F.col("payments"), lambda p: p["status"] == "approved")[0]
)

# ------------------------------------------------------------------
# Passo 3 — Explode order_items (uma linha por item)
# ------------------------------------------------------------------
df = df.withColumn("order_item", F.explode(F.col("order_items")))

# ------------------------------------------------------------------
# Passo 4 — Seleciona e tipa todas as colunas da Silver
# ------------------------------------------------------------------
df_silver = df.select(

    # --- campos raiz da order ---
    F.col("id").cast(LongType()).alias("id"),
    F.col("status").cast(StringType()).alias("status"),
    F.col("status_detail").cast(StringType()).alias("status_detail"),
    F.col("expiration_date").cast(StringType()).alias("expiration_date"),
    F.col("date_created").cast(TimestampType()).alias("date_created"),
    F.col("date_closed").cast(TimestampType()).alias("date_closed"),
    F.col("date_last_updated").cast(TimestampType()).alias("date_last_updated"),
    F.col("last_updated").cast(TimestampType()).alias("last_updated"),
    F.col("pack_id").cast(LongType()).alias("pack_id"),
    F.col("total_amount").cast(DoubleType()).alias("total_amount"),
    F.col("paid_amount").cast(DoubleType()).alias("paid_amount"),
    F.col("currency_id").cast(StringType()).alias("currency_id"),
    F.col("mediations").cast(StringType()).alias("mediations"),
    F.col("_ingested_at").cast(TimestampType()).alias("_ingested_at"),

    # --- shipping ---
    F.col("shipping.id").cast(LongType()).alias("shipping_id"),

    # --- coupon ---
    F.col("coupon.amount").cast(DoubleType()).alias("coupon_amount"),
    F.col("coupon.id").cast(StringType()).alias("coupon_id"),

    # --- buyer ---
    F.col("buyer.id").cast(LongType()).alias("buyer_id"),
    F.col("buyer.nickname").cast(StringType()).alias("buyer_nickname"),

    # --- seller ---
    F.col("seller.id").cast(LongType()).alias("seller_id"),
    F.col("seller.nickname").cast(StringType()).alias("seller_nickname"),

    # --- payment (primeiro aprovado) ---
    F.col("payment.status_code").cast(StringType()).alias("payment_status_code"),
    F.col("payment.total_paid_amount").cast(DoubleType()).alias("payment_total_paid_amount"),
    F.col("payment.operation_type").cast(StringType()).alias("payment_operation_type"),
    F.col("payment.transaction_amount").cast(DoubleType()).alias("payment_transaction_amount"),
    F.col("payment.transaction_amount_refunded").cast(DoubleType()).alias("payment_transaction_amount_refunded"),
    F.col("payment.date_approved").cast(TimestampType()).alias("payment_date_approved"),
    F.col("payment.collector.id").cast(LongType()).alias("payment_collector_id"),
    F.col("payment.coupon_id").cast(StringType()).alias("payment_coupon_id"),
    F.col("payment.installments").cast(IntegerType()).alias("payment_installments"),
    F.col("payment.authorization_code").cast(StringType()).alias("payment_authorization_code"),
    F.col("payment.taxes_amount").cast(DoubleType()).alias("payment_taxes_amount"),
    F.col("payment.id").cast(LongType()).alias("payment_id"),
    F.col("payment.date_last_modified").cast(TimestampType()).alias("payment_date_last_modified"),
    F.col("payment.coupon_amount").cast(DoubleType()).alias("payment_coupon_amount"),
    F.col("payment.available_actions").cast(StringType()).alias("payment_available_actions"),
    F.col("payment.shipping_cost").cast(DoubleType()).alias("payment_shipping_cost"),
    F.col("payment.installment_amount").cast(DoubleType()).alias("payment_installment_amount"),
    F.col("payment.date_created").cast(TimestampType()).alias("payment_date_created"),
    F.col("payment.card_id").cast(LongType()).alias("payment_card_id"),
    F.col("payment.status_detail").cast(StringType()).alias("payment_status_detail"),
    F.col("payment.issuer_id").cast(LongType()).alias("payment_issuer_id"),
    F.col("payment.payment_method_id").cast(StringType()).alias("payment_method_id"),
    F.col("payment.payment_type").cast(StringType()).alias("payment_type"),
    F.col("payment.atm_transfer_reference.transaction_id").cast(StringType()).alias("payment_atm_transaction_id"),
    F.col("payment.payer_id").cast(LongType()).alias("payment_payer_id"),
    F.col("payment.order_id").cast(LongType()).alias("payment_order_id"),
    F.col("payment.currency_id").cast(StringType()).alias("payment_currency_id"),
    F.col("payment.status").cast(StringType()).alias("payment_status"),

    # --- order_item ---
    F.col("order_item.quantity").cast(IntegerType()).alias("order_item_quantity"),
    F.col("order_item.unit_price").cast(DoubleType()).alias("order_item_unit_price"),
    F.col("order_item.gross_price").cast(DoubleType()).alias("order_item_gross_price"),
    F.col("order_item.sale_fee").cast(DoubleType()).alias("order_item_sale_fee"),
    F.col("order_item.item.id").cast(StringType()).alias("order_item_item_id"),
    F.col("order_item.item.title").cast(StringType()).alias("order_item_item_title"),
    F.col("order_item.item.category_id").cast(StringType()).alias("order_item_item_category_id"),
    F.col("order_item.item.variation_id").cast(LongType()).alias("order_item_item_variation_id"),
    F.col("order_item.item.seller_sku").cast(StringType()).alias("order_item_item_seller_sku"),
    F.col("order_item.stock.node_id").cast(StringType()).alias("order_item_stock_node_id"),

    # --- partição ---
    F.to_date(F.col("date_created")).alias("order_date"),
)

# ------------------------------------------------------------------
# Passo 5 — Deduplicação
# Chave: id (order) + order_item_item_id
# Mantém o registro com date_last_updated mais recente
# ------------------------------------------------------------------
window = Window.partitionBy("id", "order_item_item_id").orderBy(
    F.col("date_last_updated").desc()
)

df_silver = (
    df_silver
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# ------------------------------------------------------------------
# Passo 6 — Cria tabela Iceberg se não existir e faz MERGE
# ------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GLUE_DATABASE}.{GLUE_TABLE} (
        id                                  BIGINT,
        status                              STRING,
        status_detail                       STRING,
        expiration_date                     STRING,
        date_created                        TIMESTAMP,
        date_closed                         TIMESTAMP,
        date_last_updated                   TIMESTAMP,
        last_updated                        TIMESTAMP,
        pack_id                             BIGINT,
        total_amount                        DOUBLE,
        paid_amount                         DOUBLE,
        currency_id                         STRING,
        mediations                          STRING,
        _ingested_at                        TIMESTAMP,
        shipping_id                         BIGINT,
        coupon_amount                       DOUBLE,
        coupon_id                           STRING,
        buyer_id                            BIGINT,
        buyer_nickname                      STRING,
        seller_id                           BIGINT,
        seller_nickname                     STRING,
        payment_status_code                 STRING,
        payment_total_paid_amount           DOUBLE,
        payment_operation_type              STRING,
        payment_transaction_amount          DOUBLE,
        payment_transaction_amount_refunded DOUBLE,
        payment_date_approved               TIMESTAMP,
        payment_collector_id                BIGINT,
        payment_coupon_id                   STRING,
        payment_installments                INT,
        payment_authorization_code          STRING,
        payment_taxes_amount                DOUBLE,
        payment_id                          BIGINT,
        payment_date_last_modified          TIMESTAMP,
        payment_coupon_amount               DOUBLE,
        payment_available_actions           STRING,
        payment_shipping_cost               DOUBLE,
        payment_installment_amount          DOUBLE,
        payment_date_created                TIMESTAMP,
        payment_card_id                     BIGINT,
        payment_status_detail               STRING,
        payment_issuer_id                   BIGINT,
        payment_method_id                   STRING,
        payment_type                        STRING,
        payment_atm_transaction_id          STRING,
        payment_payer_id                    BIGINT,
        payment_order_id                    BIGINT,
        payment_currency_id                 STRING,
        payment_status                      STRING,
        order_item_quantity                 INT,
        order_item_unit_price               DOUBLE,
        order_item_gross_price              DOUBLE,
        order_item_sale_fee                 DOUBLE,
        order_item_item_id                  STRING,
        order_item_item_title               STRING,
        order_item_item_category_id         STRING,
        order_item_item_variation_id        BIGINT,
        order_item_item_seller_sku          STRING,
        order_item_stock_node_id            STRING,
        order_date                          DATE
    )
    USING iceberg
    LOCATION '{SILVER_PATH}'
    TBLPROPERTIES (
        'table_type'                      = 'ICEBERG',
        'format'                          = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    PARTITIONED BY (order_date)
""")

df_silver.createOrReplaceTempView("orders_staging")

spark.sql(f"""
    MERGE INTO glue_catalog.{GLUE_DATABASE}.{GLUE_TABLE} AS target
    USING orders_staging AS source
        ON  target.id                  = source.id
        AND target.order_item_item_id  = source.order_item_item_id
    WHEN MATCHED AND source.date_last_updated > target.date_last_updated THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

job.commit()