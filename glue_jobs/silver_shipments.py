"""
silver_shipments.py
===================
Glue Job — Bronze → Silver (shipments)

Lógica adaptada do script Python:
  1. Lê o JSONL da Bronze do dia
  2. Filtra o sender pelo SELLER_ID
  3. Extrai cost e save do sender correto
  4. Faz MERGE INTO na tabela Iceberg da Silver pelo shipment_id
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timezone
import boto3

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
SILVER_PATH   = f"s3://{BUCKET}/silver/mercado_livre/shipments/"
GLUE_DATABASE = "silver_mercado_livre"
GLUE_TABLE    = "shipments"
SELLER_ID     = 128571198

# Configura o Glue Catalog como catálogo Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET}/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# ------------------------------------------------------------------
# Passo 1 — Verifica se existe arquivo na partição do dia
# ------------------------------------------------------------------
today        = datetime.now(timezone.utc)
BRONZE_PATH  = (
    f"s3://{BUCKET}/bronze/mercado_livre/shipments/"
    f"year={today.year}/"
    f"month={today.month:02d}/"
    f"day={today.day:02d}/"
)

s3       = boto3.client("s3")
prefix   = (
    f"bronze/mercado_livre/shipments/"
    f"year={today.year}/"
    f"month={today.month:02d}/"
    f"day={today.day:02d}/"
)
response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)

if response.get("KeyCount", 0) == 0:
    print(f"Nenhum arquivo encontrado em {BRONZE_PATH} — nada a processar.")
    job.commit()
    sys.exit(0)

# ------------------------------------------------------------------
# Passo 2 — Lê o JSONL da Bronze do dia
# ------------------------------------------------------------------
df = spark.read.json(BRONZE_PATH)

# ------------------------------------------------------------------
# Passo 3 — Filtra o sender pelo SELLER_ID e extrai cost e save
#
# Equivalente ao script Python:
#   sender_match = next(
#       (s for s in senders if s.get("user_id") == USER_ID), None
#   )
#   cost = sender_match.get("cost")
#   save = sender_match.get("save")
# ------------------------------------------------------------------
#
# Equivalente ao script Python:
#   sender_match = next(
#       (s for s in senders if s.get("user_id") == USER_ID), None
#   )
#   cost = sender_match.get("cost")
#   save = sender_match.get("save")
# ------------------------------------------------------------------

# Explode o array de senders para filtrar pelo user_id correto
df_sender = (
    df
    .withColumn("sender", F.explode(F.col("senders")))
    .filter(F.col("sender.user_id") == SELLER_ID)
)

# ------------------------------------------------------------------
# Passo 4 — Seleciona e tipa as colunas
# ------------------------------------------------------------------
df_silver = df_sender.select(
    F.col("shipment_id").cast(LongType()).alias("shipment_id"),
    F.col("sender.cost").cast(DoubleType()).alias("cost"),
    F.col("sender.save").cast(DoubleType()).alias("save"),
    F.col("_ingested_at").cast(TimestampType()).alias("_ingested_at"),
)

# ------------------------------------------------------------------
# Passo 5 — Deduplicação — mantém o registro mais recente
# ------------------------------------------------------------------
window = Window.partitionBy("shipment_id").orderBy(F.col("_ingested_at").desc())

df_silver = (
    df_silver
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# ------------------------------------------------------------------
# Passo 6 — Cria tabela Iceberg se não existir
# ------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GLUE_DATABASE}.{GLUE_TABLE} (
        shipment_id  BIGINT,
        cost         DOUBLE,
        save         DOUBLE,
        _ingested_at TIMESTAMP
    )
    USING iceberg
    LOCATION '{SILVER_PATH}'
    TBLPROPERTIES (
        'table_type'                      = 'ICEBERG',
        'format'                          = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
""")

# ------------------------------------------------------------------
# Passo 7 — MERGE pelo shipment_id — mantém só o mais recente
# ------------------------------------------------------------------
df_silver.createOrReplaceTempView("shipments_staging")

spark.sql(f"""
    MERGE INTO glue_catalog.{GLUE_DATABASE}.{GLUE_TABLE} AS target
    USING shipments_staging AS source
        ON target.shipment_id = source.shipment_id
    WHEN MATCHED AND source._ingested_at > target._ingested_at THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

job.commit()