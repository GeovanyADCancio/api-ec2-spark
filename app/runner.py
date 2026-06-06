import subprocess
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"

def run_spark_script(script_name: str) -> dict:
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        return {"status": "error", "message": f"Script não encontrado: {script_name}"}

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        timeout=1800   # 30 min máximo
    )

    return {
        "status":      "success" if result.returncode == 0 else "error",
        "returncode":  result.returncode,
        "stdout":      result.stdout[-5000:],   # últimas 5k chars
        "stderr":      result.stderr[-2000:] if result.returncode != 0 else "",
    }