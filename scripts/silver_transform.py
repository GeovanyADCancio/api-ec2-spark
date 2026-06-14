"""
silver_transform.py
Camada Silver — Transformações e Enriquecimento

O que este script faz:
  1. Lê a tabela Iceberg bronze (transactions)
  2. Aplica regras de qualidade: remove duplicatas, filtra registros inválidos
  3. Enriquece os dados: categorias de valor, score de risco, faixas horárias
  4. Calcula métricas agregadas por conta (features para detecção de fraude)
  5. Salva a camada silver como nova tabela Iceberg particionada

Bronze → Silver:
  Bronze = dados brutos com limpeza mínima (o que chegou da fonte)
  Silver = dados confiáveis, enriquecidos, prontos para análise e ML
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ─────────────────────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────────────────────

BUCKET      = "aula-spark-emprega-dados1"
ICEBERG_WH  = f"s3a://{BUCKET}/lakehouse/warehouse"

CATALOG     = "local_catalog"
DATABASE    = "banco_digital"

# Tabelas
BRONZE_TABLE = f"{CATALOG}.{DATABASE}.transactions"
SILVER_TABLE = f"{CATALOG}.{DATABASE}.transactions_silver"


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────

def criar_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Silver Transform — Banco Digital")
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


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 1 — Criar tabela Silver
# ─────────────────────────────────────────────────────────────────────────────

def criar_tabela_silver(spark: SparkSession) -> None:
    """
    Cria a tabela silver com schema enriquecido.
    As colunas extras (faixa_valor, score_risco, etc.) não existem no bronze —
    são derivadas durante a transformação.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            transacao_id        STRING,
            data                STRING,
            ano                 INT,
            mes                 INT,
            tipo                STRING,
            canal               STRING,
            status              STRING,
            motivo_recusa       STRING,
            valor               DOUBLE,
            alerta_fraude       BOOLEAN,
            conta_origem        STRING,
            tipo_origem         STRING,
            segmento_origem     STRING,
            estado_origem       STRING,
            conta_destino       STRING,
            tipo_destino        STRING,
            estado_destino      STRING,

            -- Colunas derivadas (não existem no bronze)
            faixa_valor         STRING,   -- 'micro' / 'pequeno' / 'medio' / 'alto' / 'muito_alto'
            transferencia_interestadual BOOLEAN,  -- origem != destino
            score_risco         INT,      -- 0-100 composto por regras de negócio
            categoria_risco     STRING,   -- 'baixo' / 'medio' / 'alto' / 'critico'

            -- Features de comportamento da conta (window functions)
            qtd_transacoes_conta_dia    BIGINT,   -- volume de transações da conta no mesmo dia
            volume_conta_dia            DOUBLE,   -- soma dos valores da conta no dia
            ticket_medio_conta          DOUBLE,   -- média histórica da conta

            -- Controle de processamento
            processado_em       TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (ano, mes)
        LOCATION '{ICEBERG_WH}/{DATABASE}/transactions_silver'
        TBLPROPERTIES (
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    print("✅  Tabela silver garantida.")


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 2 — Ler bronze e aplicar qualidade
# ─────────────────────────────────────────────────────────────────────────────

def ler_e_limpar_bronze(spark: SparkSession):
    """
    Lê o bronze e aplica regras de qualidade:
    - Remove duplicatas por transacao_id
    - Filtra registros com valor nulo ou negativo
    - Filtra registros sem conta_origem
    """
    print("📖  Lendo camada bronze ...")

    df = spark.table(BRONZE_TABLE)
    total_bronze = df.count()
    print(f"    {total_bronze:,} registros na camada bronze")

    df_clean = (
        df
        # Remove duplicatas: mantém o primeiro registro de cada transacao_id
        .dropDuplicates(["transacao_id"])
        # Filtra valores inválidos
        .filter(F.col("valor").isNotNull() & (F.col("valor") > 0))
        # Filtra registros sem conta origem
        .filter(F.col("conta_origem").isNotNull())
    )

    total_clean = df_clean.count()
    descartados = total_bronze - total_clean
    print(f"    {descartados:,} registros descartados na etapa de qualidade")
    print(f"    {total_clean:,} registros válidos para transformação")

    return df_clean


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 3 — Enriquecimento: colunas derivadas
# ─────────────────────────────────────────────────────────────────────────────

def enriquecer(df):
    """
    Adiciona colunas derivadas que agregam valor analítico ao dado bruto.

    faixa_valor: segmenta transações por volume financeiro
    transferencia_interestadual: flag para cruzamentos de estado (risco maior)
    score_risco: pontuação 0-100 composta por múltiplas regras de negócio
    categoria_risco: classificação legível do score
    """
    print("🔧  Aplicando enriquecimentos ...")

    df_enriched = (
        df

        # ── Faixa de valor ────────────────────────────────────────────────
        # Segmenta transações por volume para análise de distribuição
        .withColumn("faixa_valor",
            F.when(F.col("valor") < 100,    F.lit("micro"))
             .when(F.col("valor") < 1000,   F.lit("pequeno"))
             .when(F.col("valor") < 10000,  F.lit("medio"))
             .when(F.col("valor") < 100000, F.lit("alto"))
             .otherwise(F.lit("muito_alto"))
        )

        # ── Transferência interestadual ───────────────────────────────────
        # Transações entre estados têm perfil de risco diferente
        .withColumn("transferencia_interestadual",
            F.col("estado_origem") != F.col("estado_destino")
        )

        # ── Score de risco composto (0-100) ──────────────────────────────
        # Cada regra adiciona pontos ao score:
        #   +40 se tem alerta de fraude (flag do sistema de origem)
        #   +25 se valor > 50.000 (transação de alto valor)
        #   +20 se é interestadual (origem ≠ destino)
        #   +15 se foi recusada (tentativa suspeita)
        # Score máximo teórico: 100
        .withColumn("score_risco",
            (
                F.when(F.col("alerta_fraude") == True, 40).otherwise(0) +
                F.when(F.col("valor") > 50000, 25).otherwise(0) +
                F.when(F.col("transferencia_interestadual") == True, 20).otherwise(0) +
                F.when(F.col("status") == "Recusada", 15).otherwise(0)
            ).cast(IntegerType())
        )

        # ── Categoria de risco ────────────────────────────────────────────
        .withColumn("categoria_risco",
            F.when(F.col("score_risco") >= 60, F.lit("critico"))
             .when(F.col("score_risco") >= 40, F.lit("alto"))
             .when(F.col("score_risco") >= 20, F.lit("medio"))
             .otherwise(F.lit("baixo"))
        )
    )

    return df_enriched


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 4 — Features comportamentais por conta (Window Functions)
# ─────────────────────────────────────────────────────────────────────────────

def calcular_features_conta(df):
    """
    Calcula métricas de comportamento por conta usando Window Functions.

    Estas features são fundamentais para detecção de fraude:
    - Uma conta que fez 50 transações em um dia é anômala
    - Um PIX de R$80.000 de uma conta com ticket médio de R$200 é suspeito

    Window por conta + dia: conta quantas transações e qual o volume
    Window por conta histórico: calcula o ticket médio da conta
    """
    print("📊  Calculando features comportamentais por conta ...")

    # Janela: mesma conta, mesmo dia
    w_conta_dia = (
        Window
        .partitionBy("conta_origem", "data")
    )

    # Janela: toda a história da conta (para ticket médio)
    w_conta_historico = (
        Window
        .partitionBy("conta_origem")
    )

    df_features = (
        df
        .withColumn("qtd_transacoes_conta_dia",
            F.count("transacao_id").over(w_conta_dia)
        )
        .withColumn("volume_conta_dia",
            F.round(F.sum("valor").over(w_conta_dia), 2)
        )
        .withColumn("ticket_medio_conta",
            F.round(F.avg("valor").over(w_conta_historico), 2)
        )
    )

    return df_features


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 5 — Salvar camada silver
# ─────────────────────────────────────────────────────────────────────────────

def salvar_silver(spark: SparkSession, df) -> None:
    """
    Seleciona apenas as colunas do schema silver (descarta colunas do bronze
    que não fazem parte do contrato silver, como agencia_origem) e salva.

    overwritePartitions() reescreve apenas as partições presentes no DataFrame,
    preservando partições históricas que não foram reprocessadas.
    """
    print("💾  Salvando camada silver ...")

    df_silver = df.select(
        "transacao_id", "data", "ano", "mes",
        "tipo", "canal", "status", "motivo_recusa",
        "valor", "alerta_fraude",
        "conta_origem", "tipo_origem", "segmento_origem", "estado_origem",
        "conta_destino", "tipo_destino", "estado_destino",
        "faixa_valor", "transferencia_interestadual",
        "score_risco", "categoria_risco",
        "qtd_transacoes_conta_dia", "volume_conta_dia", "ticket_medio_conta",
        F.current_timestamp().alias("processado_em"),
    )

    total = df_silver.count()

    (
        df_silver
        .writeTo(SILVER_TABLE)
        .overwritePartitions()
    )

    print(f"✅  {total:,} registros salvos na camada silver.")

    # Distribuição de risco — útil para validar o processamento
    print("\n📊  Distribuição por categoria de risco:")
    spark.table(SILVER_TABLE).groupBy("categoria_risco").count() \
        .orderBy("count", ascending=False).show()


def salvar_fraud_summary(spark: SparkSession) -> None:
    """
    Exporta um relatório agregado das transações críticas para o S3.
    A Lambda lê esse Parquet via S3 Select sem precisar de Spark.
    """
    destino = f"s3a://{BUCKET}/lakehouse/reports/fraud_summary"

    spark.table(SILVER_TABLE).filter(
        F.col("categoria_risco").isin("alto", "critico")
    ).groupBy("tipo", "estado_origem", "categoria_risco").agg(
        F.count("transacao_id").alias("qtd"),
        F.round(F.sum("valor"), 2).alias("volume_brl"),
        F.round(F.avg("score_risco"), 1).alias("score_medio"),
    ).write.mode("overwrite").parquet(destino)

    print(f"✅  fraud_summary salvo em {destino}")

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = criar_spark_session()
    try:
        criar_tabela_silver(spark)
        df = ler_e_limpar_bronze(spark)
        df = enriquecer(df)
        df = calcular_features_conta(df)
        salvar_silver(spark, df)
        salvar_fraud_summary(spark)
        print("🏁  silver_transform concluído com sucesso!")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()