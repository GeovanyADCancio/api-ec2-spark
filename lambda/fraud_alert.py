"""
fraud_alert.py — AWS Lambda Function

Responsabilidade: ler a camada silver do S3, identificar transações críticas
e publicar um resumo de alertas no SNS para notificação das equipes.

Trigger: invocada pelo Airflow via boto3 após o job silver terminar.

Variáveis de ambiente esperadas:
  BUCKET         → nome do bucket S3 (ex: aula-spark-emprega-dados1)
  SNS_TOPIC_ARN  → ARN do tópico SNS para publicar alertas
                   (se não configurado, apenas loga — útil para aula)

Como funciona:
  1. Lê os Parquets da camada silver (pasta reports/fraud_summary gerada pelo Spark)
  2. Filtra registros com categoria_risco = 'critico'
  3. Agrega por tipo de transação e estado de origem
  4. Publica resumo no SNS (ou loga se SNS não configurado)
  5. Retorna estatísticas do processamento
"""

import json
import os
import boto3
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clientes AWS — inicializados fora do handler para reutilizar entre invocações
s3_client  = boto3.client("s3")
sns_client = boto3.client("sns")


def ler_fraud_summary(bucket: str) -> list[dict]:
    """
    Lê o relatório de fraude gerado pelo Spark (Parquet → JSON via S3 Select
    ou listagem de objetos).

    Para simplificar na aula, usamos S3 Select com SQL para filtrar
    diretamente no S3 sem baixar o arquivo inteiro.
    """
    prefix = "lakehouse/reports/fraud_summary/"

    # Lista os arquivos Parquet do relatório
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get("Contents", [])

    parquet_files = [
        obj["Key"] for obj in objects
        if obj["Key"].endswith(".parquet") and obj["Size"] > 0
    ]

    if not parquet_files:
        logger.warning(f"Nenhum arquivo Parquet encontrado em s3://{bucket}/{prefix}")
        return []

    logger.info(f"Encontrados {len(parquet_files)} arquivo(s) Parquet")

    # Usa S3 Select para ler o Parquet sem baixar tudo
    # (alternativa: usar pandas + pyarrow na Lambda Layer)
    records = []
    for key in parquet_files:
        try:
            resp = s3_client.select_object_content(
                Bucket=bucket,
                Key=key,
                ExpressionType="SQL",
                Expression="""
                    SELECT
                        s.tipo,
                        s.estado_origem,
                        s.categoria_risco,
                        s.qtd,
                        s.volume_brl,
                        s.score_medio
                    FROM S3Object s
                    WHERE s.categoria_risco = 'critico'
                """,
                InputSerialization={"Parquet": {}},
                OutputSerialization={"JSON": {"RecordDelimiter": "\n"}},
            )

            for event in resp["Payload"]:
                if "Records" in event:
                    linhas = event["Records"]["Payload"].decode("utf-8").strip().split("\n")
                    for linha in linhas:
                        if linha:
                            records.append(json.loads(linha))

        except Exception as e:
            logger.error(f"Erro ao ler {key}: {e}")

    return records


def publicar_alerta(topic_arn: str, registros: list[dict], bucket: str) -> None:
    """
    Publica um resumo dos alertas críticos no SNS.
    O SNS pode entregar para email, SQS, Lambda downstream, etc.
    """
    if not registros:
        mensagem = "✅ Processamento silver concluído — nenhuma transação crítica identificada."
    else:
        total_qtd    = sum(r.get("qtd", 0) for r in registros)
        total_volume = sum(r.get("volume_brl", 0) for r in registros)

        linhas = [
            f"🚨 ALERTA DE FRAUDE — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            f"",
            f"Transações críticas identificadas: {total_qtd:,}",
            f"Volume financeiro em risco: R$ {total_volume:,.2f}",
            f"",
            f"Detalhamento por tipo e estado:",
        ]

        for r in sorted(registros, key=lambda x: x.get("volume_brl", 0), reverse=True)[:10]:
            linhas.append(
                f"  • {r.get('tipo','?')} | {r.get('estado_origem','?')} → "
                f"qtd={r.get('qtd',0):,} | "
                f"volume=R${r.get('volume_brl',0):,.2f} | "
                f"score_medio={r.get('score_medio',0):.0f}"
            )

        linhas += [
            f"",
            f"Dados completos: s3://{bucket}/lakehouse/reports/fraud_summary/",
        ]

        mensagem = "\n".join(linhas)

    logger.info(f"Publicando alerta no SNS:\n{mensagem}")

    sns_client.publish(
        TopicArn=topic_arn,
        Subject="[Banco Digital] Alerta de Fraude — Processamento Silver",
        Message=mensagem,
    )

    logger.info("✅ Alerta publicado no SNS.")


def handler(event: dict, context) -> dict:
    """
    Entry point da Lambda.

    Parâmetros do event (enviados pelo Airflow via payload):
      bucket       → nome do bucket (opcional, usa env var como fallback)
      dry_run      → se True, não publica no SNS (útil para testes)
    """
    logger.info(f"Evento recebido: {json.dumps(event)}")

    bucket      = event.get("bucket") or os.environ.get("BUCKET", "aula-spark-emprega-dados1")
    sns_topic   = os.environ.get("SNS_TOPIC_ARN", "")
    dry_run     = event.get("dry_run", False)

    inicio = datetime.now(timezone.utc)

    # 1. Ler o fraud summary gerado pelo Spark
    registros = ler_fraud_summary(bucket)

    total_criticos = len(registros)
    total_volume   = sum(r.get("volume_brl", 0) for r in registros)

    logger.info(f"Registros críticos encontrados: {total_criticos}")
    logger.info(f"Volume total em risco: R$ {total_volume:,.2f}")

    # 2. Publicar alerta (ou só logar em dry_run / sem SNS configurado)
    if dry_run:
        logger.info("dry_run=True — alerta não publicado no SNS.")
    elif sns_topic:
        publicar_alerta(sns_topic, registros, bucket)
    else:
        logger.warning(
            "SNS_TOPIC_ARN não configurado — alerta logado mas não publicado. "
            "Configure a variável de ambiente para habilitar notificações."
        )

    duracao_ms = int((datetime.now(timezone.utc) - inicio).total_seconds() * 1000)

    return {
        "statusCode": 200,
        "body": {
            "status":           "success",
            "total_criticos":   total_criticos,
            "volume_em_risco":  round(total_volume, 2),
            "sns_publicado":    bool(sns_topic and not dry_run),
            "duracao_ms":       duracao_ms,
            "bucket":           bucket,
        }
    }