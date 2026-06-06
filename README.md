# FastAPI + Spark Lakehouse — EC2

API para orquestrar jobs PySpark na EC2, expondo o `lakehouse_iceberg.py` via HTTP.  
Objetivo: acessar o Swagger pelo IP da EC2, disparar o Spark e ver os dados gravados no S3.

---

## Pré-requisitos

| O que | Onde já está |
|---|---|
| Java 11, PySpark 3.5, pyarrow, boto3 | EC2 (aula anterior) |
| IAM Role com acesso ao S3 | EC2 |
| Repositório clonado localmente | sua máquina |
| Python 3.10+ | local e EC2 |

---

## Parte 1 — Chave SSH na EC2 → GitHub (sem usuário/senha)

Faça isso **dentro da EC2** (via SSH ou console AWS).

### 1.1 Gerar a chave SSH

```bash
ssh-keygen -t ed25519 -C "ec2-mentoria" -f ~/.ssh/id_ed25519 -N ""
```

### 1.2 Exibir a chave pública

```bash
cat ~/.ssh/id_ed25519.pub
```

Copie a saída (começa com `ssh-ed25519 AAAA...`).

### 1.3 Adicionar no GitHub

1. Acesse **github.com → Settings → SSH and GPG keys → New SSH key**
2. Title: `ec2-mentoria`
3. Cole a chave pública
4. Clique **Add SSH key**

### 1.4 Testar a conexão

```bash
ssh -T git@github.com
# Resposta esperada: Hi <seu-usuario>! You've successfully authenticated...
```

### 1.5 Converter o remote do repositório para SSH (se estava em HTTPS)

```bash
# Na EC2, dentro da pasta do repositório
git remote -v                          # ver o remote atual
git remote set-url origin git@github.com:SEU_USUARIO/SEU_REPO.git
```

---

## Parte 2 — Estrutura do projeto

```
seu-repo/
├── app/
│   ├── main.py            # FastAPI — endpoints
│   └── runner.py          # lógica que chama o Spark
├── scripts/
│   └── lakehouse_iceberg.py   # script da aula anterior
├── requirements.txt
└── README.md
```

### 2.1 Criar os arquivos localmente

**`requirements.txt`**

```
fastapi>=0.111.0
uvicorn[standard]>=0.29.0
pydantic>=2.0
```

**`app/runner.py`**

```python
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
```

**`app/main.py`**

```python
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
```

> **Dica para a aula:** o `/run/lakehouse` chama o `lakehouse_iceberg.py` completo que já funciona na EC2. O `/run/ingest` é um placeholder — você pode criar um script menor depois.

---

## Parte 3 — Fluxo de trabalho: local → GitHub → EC2

A abordagem recomendada é **editar localmente, fazer push, e na EC2 apenas `git pull`**.  
A EC2 nunca edita código — ela só puxa e executa.

```
sua máquina  ──push──►  GitHub  ──pull──►  EC2
```

### 3.1 Commit e push (na sua máquina)

```bash
# Adicionar os novos arquivos
git add app/ scripts/ requirements.txt README.md

# Commit
git commit -m "feat: adiciona FastAPI para orquestrar Spark"

# Push
git push origin main
```

### 3.2 Pull na EC2

```bash
# Conectar na EC2
ssh -i sua-chave.pem ec2-user@IP_DA_EC2

# Ir até o repositório
cd /home/ec2-user/seu-repo    # ajuste o caminho

# Puxar as alterações
git pull origin main
```

---

## Parte 4 — Instalar dependências e iniciar a API na EC2

### 4.1 Instalar dependências Python

```bash
pip3 install -r requirements.txt
```

### 4.2 Iniciar o servidor (porta 8000)

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Para rodar em background (sem travar o terminal):

```bash
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 > uvicorn.log 2>&1 &
echo $! > uvicorn.pid    # salva o PID para parar depois
```

Para parar quando precisar:

```bash
kill $(cat uvicorn.pid)
```

---

## Parte 5 — Liberar a porta 8000 no Security Group da EC2

No console AWS:

1. EC2 → sua instância → **Security groups** → **Edit inbound rules**
2. **Add rule**:
   - Type: `Custom TCP`
   - Port: `8000`
   - Source: `My IP` (ou `0.0.0.0/0` se quiser abrir para todos — só para aula)
3. **Save rules**

---

## Parte 6 — Acessar o Swagger e executar o Spark

### 6.1 Abrir o Swagger

```
http://IP_DA_EC2:8000/docs
```

### 6.2 Testar a API passo a passo

**1. Verificar saúde da API**
- Clique em `GET /health` → **Try it out** → **Execute**
- Resposta esperada: `{"status": "ok"}`

**2. Disparar o pipeline completo**
- Clique em `POST /run/lakehouse` → **Try it out** → **Execute**
- Aguarde (pode levar 2–5 minutos na t3.micro)
- A resposta trará `"status": "success"` e o stdout do Spark com os resultados das queries

**3. Confirmar os dados no S3**

```bash
# Na EC2 ou na sua máquina (com AWS CLI configurado)
aws s3 ls s3://aula-spark-emprega-dados1/lakehouse/warehouse/ --recursive | head -20
aws s3 ls s3://aula-spark-emprega-dados1/lakehouse/reports/  --recursive | head -10
```

---

## Parte 7 — Inicialização local (para desenvolver e testar)

Útil para validar que a API sobe antes de fazer push.

```bash
# Na sua máquina, na raiz do projeto
pip install -r requirements.txt

# Subir a API local (Swagger em http://localhost:8000/docs)
uvicorn app.main:app --reload

# O Spark não vai executar localmente pois depende do ambiente da EC2,
# mas você consegue testar os endpoints /health e ver a estrutura da API.
```

---

## Referência rápida de comandos

```bash
# Ver logs do uvicorn em background
tail -f uvicorn.log

# Verificar se a API está rodando
curl http://localhost:8000/health

# Disparar o job via curl (sem precisar do Swagger)
curl -X POST http://IP_DA_EC2:8000/run/lakehouse

# Ver processos Spark ativos
ps aux | grep spark

# Ver arquivos gerados no S3
aws s3 ls s3://aula-spark-emprega-dados1/lakehouse/ --recursive
```

---

## Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `Connection refused` na porta 8000 | Security Group bloqueado | Parte 5 — liberar porta |
| `ModuleNotFoundError: fastapi` | Dependências não instaladas | `pip3 install -r requirements.txt` |
| Job retorna `"status": "error"` | Erro no Spark — ver `stderr` na resposta | Checar caminho do S3 e IAM Role |
| `git pull` pede usuário/senha | Remote ainda em HTTPS | Parte 1.5 — converter para SSH |
| `Permission denied (publickey)` no GitHub | Chave não adicionada | Parte 1.3 — adicionar no GitHub |
| Swagger abre mas POST trava | Job longo na t3.micro | Normal — aguardar até 5 min |