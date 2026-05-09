"""
silver_custos_produtos.py
=========================
Glue Job — PostgreSQL (custos_produtos) → Silver Iceberg

A tabela tem múltiplas linhas por SKU — uma por mês de vigência.
Chave do MERGE: sku + vigencia
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
SILVER_PATH   = f"s3://{BUCKET}/silver/financeiro_interno/custos_produtos/"
GLUE_DATABASE = "silver_financeiro"
GLUE_TABLE    = "custos_produtos"

# Configura o Glue Catalog como catálogo Iceberg
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", f"s3://{BUCKET}/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# ------------------------------------------------------------------
# Passo 1 — Lê a tabela do PostgreSQL via conexão Glue
# ------------------------------------------------------------------
df = glueContext.create_dynamic_frame.from_options(
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "connectionName":          "postgres-tcc",
        "dbtable":                 "public.custos_produtos",
    }
).toDF()

# ------------------------------------------------------------------
# Passo 2 — Seleciona e tipa as colunas
# ------------------------------------------------------------------
df_silver = df.select(
    F.col("id").cast(LongType()).alias("id"),
    F.col("sku").cast(StringType()).alias("sku"),
    F.col("produto").cast(StringType()).alias("produto"),
    F.col("marca").cast(StringType()).alias("marca"),
    F.col("preco").cast(DoubleType()).alias("preco"),
    F.col("vigencia").cast(DateType()).alias("vigencia"),
    F.col("updated_at").cast(TimestampType()).alias("updated_at"),
)

# ------------------------------------------------------------------
# Passo 3 — Deduplicação por sku + vigencia
# ------------------------------------------------------------------
window = Window.partitionBy("sku", "vigencia").orderBy(F.col("updated_at").desc())

df_silver = (
    df_silver
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# ------------------------------------------------------------------
# Passo 4 — Cria tabela Iceberg se não existir
# ------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{GLUE_DATABASE}.{GLUE_TABLE} (
        id          BIGINT,
        sku         STRING,
        produto     STRING,
        marca       STRING,
        preco       DOUBLE,
        vigencia    DATE,
        updated_at  TIMESTAMP
    )
    USING iceberg
    LOCATION '{SILVER_PATH}'
    TBLPROPERTIES (
        'table_type'                      = 'ICEBERG',
        'format'                          = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    PARTITIONED BY (vigencia)
""")

# ------------------------------------------------------------------
# Passo 5 — MERGE: sku + vigencia como chave
# ------------------------------------------------------------------
df_silver.createOrReplaceTempView("custos_staging")

spark.sql(f"""
    MERGE INTO glue_catalog.{GLUE_DATABASE}.{GLUE_TABLE} AS target
    USING custos_staging AS source
        ON  target.sku      = source.sku
        AND target.vigencia = source.vigencia
    WHEN MATCHED AND source.updated_at > target.updated_at THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

job.commit()