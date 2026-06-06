from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from app.runner import run_spark_script
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Spark Lakehouse API",
    description="Orquestra jobs PySpark na EC2 via HTTP",
    version="1.0.0",
)


@app.get("/health", tags=["infra"])
def health():
    """Verifica se a API está no ar."""
    return {"status": "ok"}


@app.post("/run/lakehouse", tags=["spark"])
def run_lakehouse():
    """
    Executa o pipeline completo do Lakehouse Iceberg:
    cria tabela, insere dados, faz UPDATE/DELETE, Time Travel e compactação.

    ⚠️ Operação bloqueante — aguarde o retorno (pode demorar alguns minutos na t3.micro).
    """
    logger.info("Iniciando lakehouse_iceberg.py ...")
    result = run_spark_script("lakehouse_iceberg.py")
    logger.info(f"Job finalizado com status: {result['status']}")
    return JSONResponse(content=result)


@app.post("/run/ingest", tags=["spark"])
def run_ingest():
    """
    Executa apenas os passos de criação de tabela e ingestão de dados (Passos 1 e 2).
    Útil para testar sem rodar o pipeline inteiro.
    """
    result = run_spark_script("ingest_only.py")
    return JSONResponse(content=result)