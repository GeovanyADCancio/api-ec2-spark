"""
ingest_only.py file
Executa apenas a criação da tabela Iceberg e a ingestão dos dados brutos.
Usado pelo endpoint POST /run/ingest da FastAPI.    dfasd
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET     = "aula-spark-emprega-dados1"
RAW_PATH   = f"s3a://{BUCKET}/raw/transactions"
ICEBERG_WH = f"s3a://{BUCKET}/lakehouse/warehouse"

CATALOG    = "local_catalog"
DATABASE   = "banco_digital"
TABLE      = "transactions"
FULL_TABLE = f"{CATALOG}.{DATABASE}.{TABLE}"


def criar_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Ingest Only — Banco Digital")
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", ICEBERG_WH)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "6g")
        .config("spark.local.dir", "/tmp/spark")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def criar_tabela(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
            transacao_id    STRING,
            data            STRING,
            ano             INT,
            mes             INT,
            tipo            STRING,
            canal           STRING,
            status          STRING,
            motivo_recusa   STRING,
            valor           DOUBLE,
            alerta_fraude   BOOLEAN,
            conta_origem    STRING,
            tipo_origem     STRING,
            segmento_origem STRING,
            estado_origem   STRING,
            agencia_origem  STRING,
            conta_destino   STRING,
            tipo_destino    STRING,
            estado_destino  STRING
        )
        USING iceberg
        PARTITIONED BY (ano, mes)
        LOCATION '{ICEBERG_WH}/{DATABASE}/{TABLE}'
        TBLPROPERTIES (
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    print(f"✅  Tabela '{FULL_TABLE}' garantida.")


def inserir_dados(spark: SparkSession) -> None:
    df = (
        spark.read.parquet(RAW_PATH)
        .withColumn("valor", F.round(F.col("valor"), 2))
        .withColumn("alerta_fraude",
                    F.when(F.col("alerta_fraude").isNull(), F.lit(False))
                     .otherwise(F.col("alerta_fraude")))
    )
    total = df.count()
    print(f"📥  {total:,} linhas lidas de {RAW_PATH}")
    df.writeTo(FULL_TABLE).append()
    print("✅  Ingestão concluída.")


def main() -> None:
    spark = criar_spark_session()
    try:
        criar_tabela(spark)
        inserir_dados(spark)
        print("🏁  ingest_only concluído com sucesso!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
