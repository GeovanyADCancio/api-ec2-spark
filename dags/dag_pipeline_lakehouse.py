"""
dag_pipeline_lakehouse.py
DAG principal do pipeline Lakehouse — Banco Digital

Orquestração:
  1. health_check     → verifica se a API na EC2 está no ar (HttpSensor)
  2. bronze_ingest    → POST /run/ingest   → cria tabela + ingere dados brutos
  3. silver_transform → POST /run/silver   → transforma, enriquece, score de risco
  4. fraud_alert      → invoca Lambda      → lê silver, publica alertas críticos no SNS

Dependências:
  health_check → bronze_ingest → silver_transform → fraud_alert

Conexões necessárias no Airflow (Admin → Connections):
  ec2_spark_api:
    Conn Type : HTTP
    Host      : http://<IP_DA_EC2>
    Port      : 8000

  aws_default:
    Conn Type : Amazon Web Services
    Extra     : {"region_name": "sa-east-1"}
    (ou usar IAM Role se o Airflow rodar em EC2/ECS com role associada)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

import json
import logging

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Configuração da DAG
# ─────────────────────────────────────────────────────────────────────────────

LAMBDA_FUNCTION_NAME = "fraud-alert-banco-digital"   # nome da função na AWS
EC2_CONN_ID          = "ec2_spark_api"               # conexão HTTP cadastrada no Airflow
AWS_CONN_ID          = "aws_default"                 # conexão AWS cadastrada no Airflow

default_args = {
    "owner":            "emprega_dados",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="pipeline_lakehouse_banco_digital",
    description="Bronze → Silver (EC2/Spark via API) → Fraud Alert (Lambda)",
    schedule_interval="0 6 * * *",   # todo dia às 06:00 BRT
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "spark", "lambda", "banco_digital"],
) as dag:

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 1 — Health check: garante que a API está no ar antes de disparar
    # ─────────────────────────────────────────────────────────────────────────

    health_check = HttpSensor(
        task_id="health_check_ec2_api",
        http_conn_id=EC2_CONN_ID,
        endpoint="/health",
        method="GET",
        response_check=lambda response: response.json().get("status") == "ok",
        poke_interval=10,       # tenta a cada 10 segundos
        timeout=60,             # desiste após 1 minuto
        mode="poke",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 2 — Bronze: ingestão dos dados brutos
    # ─────────────────────────────────────────────────────────────────────────

    bronze_ingest = SimpleHttpOperator(
        task_id="bronze_ingest",
        http_conn_id=EC2_CONN_ID,
        endpoint="/run/ingest",
        method="POST",
        headers={"Content-Type": "application/json"},
        # response_check: falha a task se o Spark retornou erro
        response_check=lambda response: _check_spark_response(response, "bronze_ingest"),
        # request_timeout: Spark pode demorar até 10 min na t3.micro
        extra_options={"timeout": 900},
        log_response=True,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 3 — Silver: transformação e enriquecimento
    # ─────────────────────────────────────────────────────────────────────────

    silver_transform = SimpleHttpOperator(
        task_id="silver_transform",
        http_conn_id=EC2_CONN_ID,
        endpoint="/run/silver",
        method="POST",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: _check_spark_response(response, "silver_transform"),
        extra_options={"timeout": 900},
        log_response=True,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 4 — Lambda: fraud alert
    # ─────────────────────────────────────────────────────────────────────────

    fraud_alert = LambdaInvokeFunctionOperator(
        task_id="fraud_alert_lambda",
        function_name=LAMBDA_FUNCTION_NAME,
        aws_conn_id=AWS_CONN_ID,
        # Payload enviado para o handler(event, context) da Lambda
        payload=json.dumps({
            "bucket":  "aula-spark-emprega-dados1",
            "dry_run": False,   # mude para True para testar sem publicar no SNS
        }),
        # invocation_type="RequestResponse" → síncrono (aguarda a Lambda terminar)
        # invocation_type="Event"           → assíncrono (dispara e não espera)
        invocation_type="RequestResponse",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TASK 5 — Log do resultado da Lambda (PythonOperator)
    # ─────────────────────────────────────────────────────────────────────────

    def log_lambda_result(**context):
        """
        Puxa o resultado da Lambda do XCom e loga as métricas principais.
        XCom é o mecanismo do Airflow para passar dados entre tasks.
        """
        ti = context["ti"]
        raw = ti.xcom_pull(task_ids="fraud_alert_lambda")

        if raw is None:
            logger.warning("Nenhum resultado da Lambda no XCom.")
            return

        # LambdaInvokeFunctionOperator devolve a resposta como string JSON
        resultado = json.loads(raw) if isinstance(raw, str) else raw
        body      = resultado.get("body", resultado)

        logger.info("=" * 50)
        logger.info("Resultado do Fraud Alert Lambda:")
        logger.info(f"  Status          : {body.get('status')}")
        logger.info(f"  Transações críticas : {body.get('total_criticos', 0):,}")
        logger.info(f"  Volume em risco : R$ {body.get('volume_em_risco', 0):,.2f}")
        logger.info(f"  SNS publicado   : {body.get('sns_publicado')}")
        logger.info(f"  Duração Lambda  : {body.get('duracao_ms')} ms")
        logger.info("=" * 50)

    log_resultado = PythonOperator(
        task_id="log_resultado_lambda",
        python_callable=log_lambda_result,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Dependências
    # ─────────────────────────────────────────────────────────────────────────

    health_check >> bronze_ingest >> silver_transform >> fraud_alert >> log_resultado


# ─────────────────────────────────────────────────────────────────────────────
# Helpers (fora do contexto do DAG)
# ─────────────────────────────────────────────────────────────────────────────

def _check_spark_response(response, task_name: str) -> bool:
    """
    Valida a resposta da API Spark.
    Retorna True (task sucesso) ou lança AirflowException (task falha).

    A API sempre retorna HTTP 200 — o status real do Spark está no body JSON:
      {"status": "success", "returncode": 0, "stdout": "...", "stderr": ""}
    """
    try:
        body = response.json()
    except Exception:
        raise AirflowException(f"[{task_name}] Resposta não é JSON válido: {response.text[:500]}")

    status     = body.get("status")
    returncode = body.get("returncode")
    stderr     = body.get("stderr", "")

    if status != "success" or returncode != 0:
        raise AirflowException(
            f"[{task_name}] Job Spark falhou | returncode={returncode}\n"
            f"stderr:\n{stderr[:1000]}"
        )

    logger.info(f"[{task_name}] Job Spark concluído com sucesso | returncode={returncode}")
    return True