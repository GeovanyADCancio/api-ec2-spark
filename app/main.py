"""
app/main.py
FastAPI — expõe os jobs PySpark como endpoints HTTP.

Rotas:
  GET  /health         → liveness check
  POST /run/ingest     → bronze: cria tabela Iceberg e ingere dados brutos
  POST /run/silver     → silver: transforma, enriquece e calcula features
  POST /run/lakehouse  → pipeline completo (aula 2)
"""

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from app.runner import run_spark_script
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Spark Lakehouse API",
    description=(
        "Orquestra jobs PySpark na EC2 via HTTP. "
        "Cada endpoint dispara um script Python que executa no Spark local. "
        "Mesmo padrão de trigger usado pelo EMR Serverless e Glue Jobs na AWS."
    ),
    version="1.0.0",
)


# ─────────────────────────────────────────────────────────────────────────────
# Infra
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["infra"])
def health():
    """Verifica se a API está no ar. Usado pelo Airflow HttpSensor."""
    return {"status": "ok"}


# ─────────────────────────────────────────────────────────────────────────────
# Spark Jobs
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/run/ingest", tags=["spark"])
def run_ingest():
    """
    **Camada Bronze** — Ingestão dos dados brutos.

    - Cria a tabela Iceberg `transactions` (se não existir)
    - Lê os Parquets de `s3://aula-spark-emprega-dados1/raw/transactions`
    - Insere na tabela Iceberg com `.writeTo().append()`

    ⚠️ Operação bloqueante — aguarda o Spark terminar (2-4 min na t3.micro).
    """
    logger.info("POST /run/ingest — iniciando ingest_only.py")
    result = run_spark_script("ingest_only.py")
    logger.info(f"ingest finalizado | status={result['status']} | returncode={result['returncode']}")
    return JSONResponse(content=result)


@app.post("/run/silver", tags=["spark"])
def run_silver():
    """
    **Camada Silver** — Transformação e enriquecimento.

    Lê a tabela bronze `transactions` e produz `transactions_silver` com:
    - Remoção de duplicatas e registros inválidos (qualidade)
    - `faixa_valor`: segmentação por volume (micro / pequeno / medio / alto / muito_alto)
    - `transferencia_interestadual`: flag origem ≠ destino
    - `score_risco`: pontuação 0-100 composta por regras de negócio
    - `categoria_risco`: baixo / medio / alto / critico
    - `qtd_transacoes_conta_dia`: volume diário da conta (Window Function)
    - `volume_conta_dia`: soma dos valores da conta no dia
    - `ticket_medio_conta`: média histórica da conta

    ⚠️ Operação bloqueante — aguarda o Spark terminar (3-6 min na t3.micro).
    """
    logger.info("POST /run/silver — iniciando silver_transform.py")
    result = run_spark_script("silver_transform.py")
    logger.info(f"silver finalizado | status={result['status']} | returncode={result['returncode']}")
    return JSONResponse(content=result)


@app.post("/run/lakehouse", tags=["spark"])
def run_lakehouse():
    """
    **Pipeline completo** — Todos os 8 passos da Aula 2.

    Cria tabela → ingere → consultas analíticas → UPDATE → DELETE
    → Time Travel → compactação → relatório gerencial.

    ⚠️ Operação bloqueante — pode demorar 10-15 min na t3.micro.
    """
    logger.info("POST /run/lakehouse — iniciando lakehouse_iceberg.py")
    result = run_spark_script("lakehouse_iceberg.py")
    logger.info(f"lakehouse finalizado | status={result['status']} | returncode={result['returncode']}")
    return JSONResponse(content=result)