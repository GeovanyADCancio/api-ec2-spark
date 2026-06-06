"""
lakehouse_iceberg.py
Aula 2 — Construindo um Lakehouse com Apache Iceberg no S3

O que este script demonstra:
  1. Criar uma tabela Iceberg no S3 (o "banco de dados" do Lakehouse)
  2. Inserir dados em lote (batch insert)
  3. Consultar com SQL — filtros, agrupamentos, janelas
  4. Atualizar registros (UPDATE) — impossível em Parquet simples
  5. Deletar registros (DELETE) — também impossível em Parquet simples
  6. Time Travel — ler versões antigas da tabela
  7. Compactação de arquivos pequenos (rewrite_data_files)
  8. Expirar snapshots antigos (manutenção do Lakehouse)

Catálogo utilizado: HadoopCatalog
  Armazena os metadados diretamente no S3, sem depender do AWS Glue.
  É a opção mais simples para aprendizado — zero configuração de IAM extra.

Pré-requisitos no EC2 (já instalados na aula anterior):
  - Java 11, PySpark 3.5, pyarrow, boto3
  - JARs do S3A na pasta de jars do PySpark

Execute:
    python3 lakehouse_iceberg.py
"""

from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────────────────────────────────────

BUCKET     = "aula-spark-emprega-dados1"
RAW_PATH   = f"s3a://{BUCKET}/raw/transactions" #s3a protocolo de conector hadoop para s3
ICEBERG_WH = f"s3a://{BUCKET}/lakehouse/warehouse"

# HadoopCatalog: metadados ficam no próprio S3 — sem Glue, sem STS, sem JAR extra
CATALOG    = "local_catalog"
DATABASE   = "banco_digital"
TABLE      = "transactions"
FULL_TABLE = f"{CATALOG}.{DATABASE}.{TABLE}"


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────

def criar_spark_session() -> SparkSession:
    """
    Cria a SparkSession com suporte a Apache Iceberg usando HadoopCatalog.

    Por que HadoopCatalog e não GlueCatalog?
    ─────────────────────────────────────────
    O GlueCatalog exige o JAR do AWS STS (software.amazon.awssdk:sts),
    que não é resolvido automaticamente pelo Iceberg 1.5 + Spark 3.5.
    O HadoopCatalog armazena os metadados diretamente no S3 como arquivos
    JSON — sem serviço externo, sem permissão adicional de IAM.
    Em produção, o Glue ou o AWS Glue Catalog seriam usados; para aprendizado,
    o HadoopCatalog é idêntico do ponto de vista do desenvolvedor.

    Os JARs do Iceberg são baixados do Maven automaticamente na primeira
    execução através de spark.jars.packages.
    """
    print("⚙️  Iniciando SparkSession com suporte a Iceberg …")

    spark = (
        SparkSession.builder
        .appName("Lakehouse Iceberg — Banco Digital")

        # ── Iceberg runtime (único JAR necessário) ───────────────────────
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        )

        # ── Extensões SQL: habilita UPDATE, DELETE, MERGE INTO ───────────
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

        # ── HadoopCatalog: metadados gravados no S3 como arquivos JSON ───
        .config(f"spark.sql.catalog.{CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", ICEBERG_WH)

        # ── Ajustes para t3.micro ────────────────────
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "6g")
        .config("spark.local.dir", "/tmp/spark")

        # ── S3A ──────────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("✅  SparkSession criada.\n")
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def titulo(texto: str) -> None:
    linha = "─" * 60
    print(f"\n{linha}")
    print(f"  {texto}")
    print(f"{linha}")


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 1 — Criar a tabela Iceberg
# ─────────────────────────────────────────────────────────────────────────────

def criar_tabela_iceberg(spark: SparkSession) -> None:
    """
    Cria o namespace (equivalente ao database) e a tabela Iceberg.

    Por que Iceberg e não Parquet simples?
    ──────────────────────────────────────
    • Parquet é imutável depois de escrito: para "atualizar" um dado é
      preciso reescrever o arquivo inteiro manualmente.
    • Iceberg adiciona uma camada de metadados (snapshots) que permite
      UPDATE, DELETE e MERGE diretamente — igual a um banco de dados.
    • Cada operação gera um novo snapshot, habilitando Time Travel.
    • Os dados continuam em Parquet no S3 — o Iceberg só gerencia quais
      arquivos fazem parte de cada versão da tabela.
    """
    titulo("PASSO 1 — Criando namespace e tabela Iceberg")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DATABASE}")
    print(f"✅  Namespace '{DATABASE}' garantido.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
            transacao_id    STRING     COMMENT 'UUID único da transação',
            data            STRING     COMMENT 'Data da transação (YYYY-MM-DD)',
            ano             INT        COMMENT 'Ano — coluna de partição',
            mes             INT        COMMENT 'Mês — coluna de partição',
            tipo            STRING     COMMENT 'Tipo da transação',
            canal           STRING     COMMENT 'Canal de entrada',
            status          STRING     COMMENT 'Status final da transação',
            motivo_recusa   STRING     COMMENT 'Preenchido somente se Recusada',
            valor           DOUBLE     COMMENT 'Valor em BRL',
            alerta_fraude   BOOLEAN    COMMENT 'Flag de alerta de fraude',
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
            'write.format.default'           = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    print(f"✅  Tabela '{FULL_TABLE}' criada (ou já existia).\n")


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 2 — Carregar os dados brutos do S3 e inserir na tabela Iceberg
# ─────────────────────────────────────────────────────────────────────────────

def inserir_dados(spark: SparkSession) -> None:
    """
    Lê os Parquets brutos (gerados pelo transactions_generation.py),
    aplica uma limpeza mínima e insere na tabela Iceberg.

    writeTo().append() = INSERT INTO — adiciona dados sem apagar os anteriores.
    Cada chamada gera um novo snapshot no histórico da tabela.
    """
    titulo("PASSO 2 — Carregando dados brutos e inserindo na tabela Iceberg")

    df_raw = (
        spark.read
        .parquet(RAW_PATH)
        .withColumn("valor", F.round(F.col("valor"), 2))
        .withColumn(
            "alerta_fraude",
            F.when(F.col("alerta_fraude").isNull(), F.lit(False))
             .otherwise(F.col("alerta_fraude"))
        )
    )

    total = df_raw.count() # dispara a execução real
    print(f"📥  {total:,} linhas lidas de {RAW_PATH}")
    print("    Schema dos dados brutos:")
    df_raw.printSchema()

    df_raw.writeTo(FULL_TABLE).append()
    print(f"✅  Dados inseridos na tabela Iceberg.\n")


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 3 — Consultas analíticas: o Lakehouse se comporta como um banco
# ─────────────────────────────────────────────────────────────────────────────

def consultas_analiticas(spark: SparkSession) -> None:
    """
    Demonstra que a tabela Iceberg aceita SQL completo:
    GROUP BY, CTEs, window functions — exatamente como em um banco de dados.
    """
    titulo("PASSO 3 — Consultas analíticas (SQL sobre Iceberg)")

    # 3.1 — Resumo geral
    print("\n📊  3.1  Resumo geral das transações aprovadas")
    spark.sql(f"""
        SELECT
            COUNT(*)                        AS total_transacoes,
            COUNT(DISTINCT conta_origem)    AS clientes_ativos,
            ROUND(SUM(valor), 2)            AS volume_total_brl,
            ROUND(AVG(valor), 2)            AS ticket_medio_brl
        FROM {FULL_TABLE}
        WHERE status = 'Aprovada'
    """).show()

    # 3.2 — Top 5 tipos por volume financeiro
    print("\n📊  3.2  Top 5 tipos de transação por volume financeiro")
    spark.sql(f"""
        SELECT
            tipo,
            COUNT(*)                AS qtd,
            ROUND(SUM(valor), 2)    AS volume_brl
        FROM {FULL_TABLE}
        WHERE status = 'Aprovada'
        GROUP BY tipo
        ORDER BY volume_brl DESC
        LIMIT 5
    """).show()

    # 3.3 — Taxa de recusa por canal
    print("\n📊  3.3  Taxa de recusa por canal")
    spark.sql(f"""
        SELECT
            canal,
            COUNT(*)                                                    AS total,
            SUM(CASE WHEN status = 'Recusada' THEN 1 ELSE 0 END)       AS recusadas,
            ROUND(
                100.0 * SUM(CASE WHEN status = 'Recusada' THEN 1 ELSE 0 END)
                / COUNT(*), 2
            )                                                           AS pct_recusa
        FROM {FULL_TABLE}
        GROUP BY canal
        ORDER BY pct_recusa DESC
    """).show()

    # 3.4 — Evolução mensal PIX vs TED
    print("\n📊  3.4  Evolução mensal — PIX vs TED (aprovadas)")
    spark.sql(f"""
        SELECT
            ano,
            mes,
            tipo,
            ROUND(SUM(valor), 2)    AS volume_brl,
            COUNT(*)                AS qtd
        FROM {FULL_TABLE}
        WHERE tipo IN ('PIX', 'TED') AND status = 'Aprovada'
        GROUP BY ano, mes, tipo
        ORDER BY ano, mes, tipo
    """).show(40)

    # 3.5 — Window function: ranking de clientes no mês mais recente
    print("\n📊  3.5  Top 10 contas por volume — mês mais recente (Window Function)")
    spark.sql(f"""
        WITH ultimo_mes AS (
            SELECT MAX(ano) AS ano, MAX(mes) AS mes
            FROM {FULL_TABLE}
        ),
        volume_clientes AS (
            SELECT
                t.conta_origem,
                t.segmento_origem,
                ROUND(SUM(t.valor), 2)  AS volume_brl,
                COUNT(*)                AS qtd_transacoes
            FROM {FULL_TABLE} t
            JOIN ultimo_mes u ON t.ano = u.ano AND t.mes = u.mes
            WHERE t.status = 'Aprovada'
            GROUP BY t.conta_origem, t.segmento_origem
        )
        SELECT
            RANK() OVER (ORDER BY volume_brl DESC)   AS ranking,
            conta_origem,
            segmento_origem,
            volume_brl,
            qtd_transacoes
        FROM volume_clientes
        LIMIT 10
    """).show()


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 4 — UPDATE: marcar transações suspeitas como Bloqueada
# ─────────────────────────────────────────────────────────────────────────────

def demonstrar_update(spark: SparkSession) -> None:
    """
    UPDATE é a primeira operação impossível em Parquet simples.

    Caso de uso real: o sistema de antifraude identificou transações
    suspeitas após o processamento inicial. Precisamos mudar o status
    dessas transações de 'Aprovada' para 'Bloqueada'.

    Em Parquet puro: seria necessário reescrever todos os arquivos afetados
    manualmente. Com Iceberg: uma linha de SQL resolve — o Iceberg reescreve
    só os arquivos que contêm registros afetados e gera um novo snapshot.
    """
    titulo("PASSO 4 — UPDATE (impossível em Parquet simples!)")

    antes = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM {FULL_TABLE}
        WHERE alerta_fraude = true AND status = 'Aprovada'
    """).collect()[0]["total"]
    print(f"  Transações com alerta de fraude ainda 'Aprovada': {antes:,}")

    print("  Executando UPDATE …")
    spark.sql(f"""
        UPDATE {FULL_TABLE}
        SET    status = 'Bloqueada'
        WHERE  alerta_fraude = true
          AND  status        = 'Aprovada'
    """)

    depois = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM {FULL_TABLE}
        WHERE status = 'Bloqueada'
    """).collect()[0]["total"]
    print(f"✅  UPDATE concluído — {depois:,} transações agora com status 'Bloqueada'.\n")


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 5 — DELETE: remover dados expirados (LGPD)
# ─────────────────────────────────────────────────────────────────────────────

def demonstrar_delete(spark: SparkSession) -> None:
    """
    DELETE também é impossível em Parquet simples.

    Caso de uso real: requisição de exclusão de dados por LGPD
    (Lei Geral de Proteção de Dados) — clientes podem solicitar que
    seu histórico seja apagado. Aqui simulamos deletando transações
    'Pendente' de antes de 2025 que já passaram do prazo de liquidação.
    """
    titulo("PASSO 5 — DELETE (LGPD / dados expirados)")

    antes = spark.sql(f"""
        SELECT COUNT(*) AS total
        FROM {FULL_TABLE}
        WHERE ano = 2026 AND mes = 1
    """).collect()[0]["total"]
    print(f"  Transações de janeiro/2026 encontradas: {antes:,}")

    print("  Executando DELETE …")
    spark.sql(f"""
        DELETE FROM {FULL_TABLE}
        WHERE ano = 2026
            AND mes = 1
    """)

    depois = spark.sql(f"""
        SELECT COUNT(*) AS total FROM {FULL_TABLE}
    """).collect()[0]["total"]
    print(f"✅  DELETE concluído — tabela agora com {depois:,} linhas.\n")


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 6 — TIME TRAVEL: consultar versões anteriores da tabela
# ─────────────────────────────────────────────────────────────────────────────

def demonstrar_time_travel(spark: SparkSession) -> None:
    """
    Time Travel é o recurso mais impressionante do Iceberg.

    Cada INSERT, UPDATE e DELETE gerou um snapshot — uma "fotografia"
    dos dados naquele momento. Podemos consultar qualquer versão
    anterior sem desfazer nada. É o equivalente ao 'git log' + 'git
    checkout <commit>' aplicado a dados.

    Casos de uso reais:
      • Auditoria: "como estavam esses dados ontem?"
      • Rollback: "o UPDATE foi errado, quero restaurar"
      • Reprodutibilidade: treinar um modelo ML com os dados de ontem
    """
    titulo("PASSO 6 — TIME TRAVEL (o 'git checkout' dos dados!)")

    # Lista todos os snapshots disponíveis
    print("  Histórico de snapshots da tabela:")
    spark.sql(f"""
        SELECT
            snapshot_id,
            committed_at,
            operation,
            summary['added-records']    AS registros_adicionados,
            summary['deleted-records']  AS registros_deletados
        FROM {FULL_TABLE}.snapshots
        ORDER BY committed_at
    """).show(20, truncate=False)

    # Pegar o ID do snapshot mais antigo (logo após o INSERT inicial)
    snapshots = spark.sql(f"""
        SELECT snapshot_id, committed_at
        FROM {FULL_TABLE}.snapshots
        ORDER BY committed_at
    """).collect()

    if len(snapshots) < 2:
        print("  ⚠️  Apenas um snapshot disponível. Execute os passos anteriores primeiro.")
        return

    snapshot_original = snapshots[0]["snapshot_id"]
    print(f"\n  📸  Snapshot original (ID={snapshot_original}) — logo após o INSERT:")
    spark.sql(f"""
        SELECT status, COUNT(*) AS total
        FROM {FULL_TABLE} VERSION AS OF {snapshot_original}
        GROUP BY status
        ORDER BY total DESC
    """).show()

    print("  📸  Estado ATUAL (após UPDATE e DELETE):")
    spark.sql(f"""
        SELECT status, COUNT(*) AS total
        FROM {FULL_TABLE}
        GROUP BY status
        ORDER BY total DESC
    """).show()

    print("✅  Time Travel demonstrado — mesma tabela, versões diferentes!\n")


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 7 — Manutenção: compactação e expiração de snapshots
# ─────────────────────────────────────────────────────────────────────────────

def manutencao_lakehouse(spark: SparkSession) -> None:
    """
    Com o tempo, UPDATE/DELETE geram muitos arquivos pequenos ("small files")
    e snapshots antigos que consomem espaço. A manutenção periódica
    mantém o Lakehouse rápido e barato.

    rewrite_data_files → compacta arquivos pequenos em arquivos maiores
    expire_snapshots   → remove snapshots antigos liberando espaço no S3
    """
    titulo("PASSO 7 — Manutenção (compactação + expiração de snapshots)")

    print("  🔧  Compactando arquivos pequenos …")
    spark.sql(f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table   => '{DATABASE}.{TABLE}',
            options => map(
                'target-file-size-bytes', '134217728',
                'min-input-files',        '5'
            )
        )
    """).show()

    print("\n  🗑️  Expirando snapshots com mais de 7 dias …")
    limite = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
        CALL {CATALOG}.system.expire_snapshots(
            table        => '{DATABASE}.{TABLE}',
            older_than   => TIMESTAMP '{limite}',
            retain_last  => 2
        )
    """).show()

    print("✅  Manutenção concluída.\n")


# ─────────────────────────────────────────────────────────────────────────────
# PASSO 8 — Relatório final exportado para o S3
# ─────────────────────────────────────────────────────────────────────────────

def salvar_relatorio(spark: SparkSession) -> None:
    """
    Exporta um relatório gerencial agregado para o S3 em formato Parquet,
    pronto para ser consumido por ferramentas de BI ou pelo Athena.
    """
    titulo("PASSO 8 — Exportando relatório gerencial para o S3")

    df_relatorio = spark.sql(f"""
        SELECT
            ano,
            mes,
            tipo,
            canal,
            status,
            COUNT(*)                        AS qtd,
            ROUND(SUM(valor), 2)            AS volume_brl,
            ROUND(AVG(valor), 2)            AS ticket_medio_brl,
            ROUND(MIN(valor), 2)            AS menor_valor_brl,
            ROUND(MAX(valor), 2)            AS maior_valor_brl
        FROM {FULL_TABLE}
        GROUP BY ano, mes, tipo, canal, status
        ORDER BY ano, mes, volume_brl DESC
    """)

    destino = f"s3a://{BUCKET}/lakehouse/reports/relatorio_gerencial"
    (
        df_relatorio
        .write
        .mode("overwrite")
        .partitionBy("ano", "mes")
        .parquet(destino)
    )
    print(f"✅  Relatório salvo em {destino}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    spark = criar_spark_session()

    try:
        criar_tabela_iceberg(spark)    # Passo 1
        inserir_dados(spark)           # Passo 2
        consultas_analiticas(spark)    # Passo 3
        demonstrar_update(spark)       # Passo 4
        demonstrar_delete(spark)       # Passo 5
        demonstrar_time_travel(spark)  # Passo 6
        manutencao_lakehouse(spark)    # Passo 7
        salvar_relatorio(spark)        # Passo 8

        print("=" * 60)
        print("  🏁  Script concluído com sucesso!")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()