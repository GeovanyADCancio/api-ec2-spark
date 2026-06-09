import subprocess
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts" # aponta para api-ec2-spark/scripts/ independente de onde o uvicorn foi iniciado.

def run_spark_script(script_name: str) -> dict:
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        return {"status": "error", "message": f"Script não encontrado: {script_name}"}

    # subprocess.run([python3, scripts/ingest_only.py])
    result = subprocess.run( # abre um processo filho e aguarda ele terminar 
        [sys.executable, str(script_path)], # caminho do Python atual (o mesmo que está rodando o uvicorn
        capture_output=True, # redireciona stdout e stderr para variáveis em vez de imprimir no terminal
        text=True, # decodifica os bytes para string automaticamente
        timeout=1800   # 30 min máximo
    )

    return {
        "status":      "success" if result.returncode == 0 else "error",
        "returncode":  result.returncode,
        "stdout":      result.stdout[-5000:],   # últimas 5k chars
        "stderr":      result.stderr[-2000:] if result.returncode != 0 else "",
    }