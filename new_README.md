# Aula 3 — Orquestração com Airflow: EC2 + Lambda

Pipeline completo:  
**Airflow (local/Docker)** → **FastAPI na EC2** → **Spark (Bronze + Silver)** → **Lambda (Fraud Alert)**

---

## Visão geral da arquitetura

```
┌─────────────────────────────────────────────────────────┐
│  Docker Compose (local)                                  │
│                                                          │
│  ┌──────────────┐    ┌───────────────────────────────┐  │
│  │   Airflow    │    │  PostgreSQL (metadados)        │  │
│  │  Webserver   │    │  DAGs, XComs, histórico        │  │
│  │  Scheduler   │    └───────────────────────────────┘  │
│  └──────┬───────┘                                        │
└─────────┼───────────────────────────────────────────────┘
          │
          │  HTTP POST /run/ingest
          │  HTTP POST /run/silver
          ▼
┌─────────────────────────┐
│  EC2 (sa-east-1)        │
│  FastAPI :8000          │
│  └─ Spark (PySpark)     │
│     └─ IAM Role →  S3   │
└─────────────────────────┘
          │
          │  S3: lakehouse/warehouse/
          ▼
┌─────────────────────────┐
│  AWS Lambda             │       ┌──────────┐
│  fraud_alert.py         │──────▶│  SNS     │
│  (lê silver, alerta)    │       │ (email)  │
└─────────────────────────┘       └──────────┘
```

---

## Parte 1 — IAM: permissões necessárias

### 1.1 Role da EC2 (já existe)

A `AulaSparkEC2Role1` já está associada à EC2 e dá acesso ao S3.  
Verifique se está associada à instância:

```
AWS Console → EC2 → sua instância → Actions → Security → Modify IAM role
```

Confirme que `AulaSparkEC2Role1` aparece. Se não estiver associada, associe agora.

### 1.2 Adicionar permissão para a Lambda invocar o SNS (opcional)

Se for usar notificação SNS, a Lambda precisa de permissão para `sns:Publish`.  
Crie uma policy inline na role da Lambda (Parte 2 cobre isso).

### 1.3 Credenciais AWS para o Airflow (Docker local)

O Airflow rodando no Docker precisa de credenciais para invocar a Lambda.  
Use um IAM User com permissão mínima — **não use as credenciais root**.

**Criar IAM User para o Airflow:**

```
IAM → Users → Create user
  Nome: airflow-local
  
IAM → Users → airflow-local → Add permissions → Attach policies directly
  Buscar e anexar: AWSLambda_FullAccess  (ou criar policy mínima abaixo)
```

**Policy mínima recomendada (mais seguro que Full Access):**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction",
        "lambda:GetFunction"
      ],
      "Resource": "arn:aws:lambda:sa-east-1:938822141434:function:fraud-alert-banco-digital"
    }
  ]
}
```

**Gerar Access Key:**

```
IAM → Users → airflow-local → Security credentials → Create access key
  Use case: Application running outside AWS
  
Anote:
  Access key ID:     AKIA...
  Secret access key: (aparece só uma vez — copie agora)
```

---

## Parte 2 — Lambda: criar e configurar

### 2.1 Criar a função

```
AWS Console → Lambda → Create function
  Author from scratch
  Function name : fraud-alert-banco-digital
  Runtime       : Python 3.12
  Architecture  : x86_64

Permissions:
  Create a new role with basic Lambda permissions
  (isso cria a AWSLambdaBasicExecutionRole automaticamente)
```

### 2.2 Fazer upload do código

No console da Lambda, em **Code**:

```
Actions → Upload a .zip file
```

Crie o zip localmente:

```bash
cd lambda/
zip fraud_alert.zip fraud_alert.py
```

Faça upload do `fraud_alert.zip`.

Confirme que o **Handler** está configurado como:

```
fraud_alert.handler
```

### 2.3 Configurar variáveis de ambiente

```
Lambda → Configuration → Environment variables → Edit → Add
```

| Key | Value |
|---|---|
| `BUCKET` | `aula-spark-emprega-dados1` |
| `SNS_TOPIC_ARN` | ARN do tópico SNS (deixe vazio na aula para só logar) |

### 2.4 Aumentar timeout e memória

O padrão (3s / 128MB) não é suficiente para ler Parquet do S3:

```
Lambda → Configuration → General configuration → Edit
  Memory : 512 MB
  Timeout: 1 min
```

### 2.5 Permissão da Lambda para ler o S3

A role criada no passo 2.1 precisa de acesso ao S3:

```
IAM → Roles → (role criada automaticamente, ex: fraud-alert-banco-digital-role-xxxx)
  → Add permissions → Attach policies
  → Buscar: AmazonS3ReadOnlyAccess → Attach
```

### 2.6 Testar a Lambda no console

```
Lambda → Test → Create new test event
```

Payload de teste:

```json
{
  "bucket": "aula-spark-emprega-dados1",
  "dry_run": true
}
```

`dry_run: true` faz a Lambda rodar sem publicar no SNS — bom para validar na aula.

---

## Parte 3 — Script silver: gerar o fraud_summary para a Lambda

O `silver_transform.py` atual grava a tabela Iceberg silver mas não gera o `fraud_summary` que a Lambda lê.  
Adicione esta função ao final do `main()` em `scripts/silver_transform.py`:

```python
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
```

E chame no `main()`:

```python
def main() -> None:
    spark = criar_spark_session()
    try:
        criar_tabela_silver(spark)
        df = ler_e_limpar_bronze(spark)
        df = enriquecer(df)
        df = calcular_features_conta(df)
        salvar_silver(spark, df)
        salvar_fraud_summary(spark)        # <── adicionar esta linha
        print("🏁  silver_transform concluído com sucesso!")
    finally:
        spark.stop()
```

---

## Parte 4 — Configurar o docker-compose

### 4.1 Estrutura de pastas esperada

```
projeto-airflow/
├── docker-compose.yml
├── Dockerfile.airflow
├── .env                        ← credenciais AWS (não commitar)
├── dags/
│   └── dag_pipeline_lakehouse.py
└── logs/
```

### 4.2 Criar o arquivo .env

Crie o arquivo `.env` na mesma pasta do `docker-compose.yml`:

```bash
# .env — NÃO commitar este arquivo (adicione ao .gitignore)

AWS_ACCESS_KEY_ID=AKIA...           # access key do airflow-local (Parte 1.3)
AWS_SECRET_ACCESS_KEY=...           # secret key do airflow-local
AWS_DEFAULT_REGION=sa-east-1
AIRFLOW_UID=50000
```

O `docker-compose.yml` já referencia essas variáveis com `${AWS_ACCESS_KEY_ID:-}`.

### 4.3 Criar o Dockerfile.airflow

```dockerfile
FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-http==4.10.0 \
    apache-airflow-providers-amazon==8.20.0
```

### 4.4 Colocar a DAG na pasta correta

```bash
# Na pasta do docker-compose
cp dag_pipeline_lakehouse.py dags/dag_pipeline_lakehouse.py
```

O volume `./dags:/opt/airflow/dags` no docker-compose sincroniza automaticamente.

---

## Parte 5 — Atualizar o IP da EC2 na DAG

Antes de subir o Airflow, edite a DAG para colocar o IP real da EC2:

```bash
# Pegue o IP público da EC2
AWS Console → EC2 → sua instância → Public IPv4 address
```

No `docker-compose.yml`, o `airflow-init` já cria a conexão `ec2_spark_api`.  
Troque `IP_DA_EC2` pelo IP real:

```yaml
airflow connections add ec2_spark_api \
  --conn-type http \
  --conn-host http://15.228.17.241 \   # ← seu IP aqui
  --conn-port 8000 || true
```

**Ou cadastre manualmente após subir** (mais fácil):

```
Airflow UI → Admin → Connections → + (Add)
  Conn Id   : ec2_spark_api
  Conn Type : HTTP
  Host      : http://15.228.17.241
  Port      : 8000
```

---

## Parte 6 — Subir o Airflow

```bash
# Na pasta do docker-compose
docker compose up --build -d
```

Aguarde os containers subirem (30-60 segundos) e acesse:

```
http://localhost:8080
  usuário: admin
  senha  : admin
```

Verifique os containers:

```bash
docker compose ps
```

Todos devem estar `healthy` ou `running`.

---

## Parte 7 — Cadastrar a conexão AWS no Airflow

```
Airflow UI → Admin → Connections → + (Add)
  Conn Id   : aws_default
  Conn Type : Amazon Web Services
  AWS Access Key ID    : AKIA...    (access key do airflow-local)
  AWS Secret Access Key: ...
  Extra: {"region_name": "sa-east-1"}
```

---

## Parte 8 — Garantir que a API da EC2 está rodando

Na EC2:

```bash
cd /home/ec2-user/api-ec2-spark

# Se não estiver rodando
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 > uvicorn.log 2>&1 &
echo $! > uvicorn.pid

# Verificar
curl http://localhost:8000/health
# {"status": "ok"}
```

Do seu computador local:

```bash
curl http://IP_DA_EC2:8000/health
```

---

## Parte 9 — Executar o pipeline pelo Airflow

### 9.1 Ativar a DAG

```
Airflow UI → DAGs → pipeline_lakehouse_banco_digital
  → toggle (ativar) → Trigger DAG (ícone de play)
```

### 9.2 Acompanhar a execução

```
Airflow UI → DAGs → pipeline_lakehouse_banco_digital → Graph View
```

Sequência esperada:

```
health_check_ec2_api   → verde (API respondeu /health)
       ↓
bronze_ingest          → verde (Spark criou tabela + ingestou ~2-4 min)
       ↓
silver_transform       → verde (Spark transformou + gerou fraud_summary ~3-6 min)
       ↓
fraud_alert_lambda     → verde (Lambda leu S3 e logou/publicou alertas)
       ↓
log_resultado_lambda   → verde (XCom logou as métricas)
```

### 9.3 Ver os logs de cada task

```
Airflow UI → DAG Run → clique na task → Log
```

No log do `bronze_ingest` você verá o stdout do Spark com os prints do script.  
No log do `fraud_alert_lambda` você verá o retorno da Lambda com `total_criticos` e `volume_em_risco`.

---

## Parte 10 — Validar os dados no S3

```bash
# Camada bronze (tabela Iceberg)
aws s3 ls s3://aula-spark-emprega-dados1/lakehouse/warehouse/banco_digital/transactions/ --recursive | head -10

# Camada silver
aws s3 ls s3://aula-spark-emprega-dados1/lakehouse/warehouse/banco_digital/transactions_silver/ --recursive | head -10

# Fraud summary (input da Lambda)
aws s3 ls s3://aula-spark-emprega-dados1/lakehouse/reports/fraud_summary/ --recursive
```

---

## Referência rápida

```bash
# Subir Airflow
docker compose up --build -d

# Ver logs do scheduler (onde as tasks executam)
docker compose logs -f airflow-scheduler

# Parar tudo
docker compose down

# Parar e apagar volumes (banco do Airflow)
docker compose down -v

# Ver logs da API na EC2
tail -f /home/ec2-user/api-ec2-spark/uvicorn.log

# Parar a API na EC2
kill $(cat /home/ec2-user/api-ec2-spark/uvicorn.pid)
```

---

## Troubleshooting

| Sintoma | Causa | Solução |
|---|---|---|
| `health_check` timeout | EC2 fora do ar ou porta 8000 fechada | Verificar Security Group + uvicorn rodando |
| `bronze_ingest` falha com `returncode != 0` | Erro no Spark — ver Log da task | Log mostra stderr do Python/Spark |
| `fraud_alert_lambda` `ResourceNotFoundException` | Nome da função errado | Confirmar `LAMBDA_FUNCTION_NAME` na DAG |
| `fraud_alert_lambda` `AccessDeniedException` | Credenciais sem permissão de invoke | Revisar policy do airflow-local (Parte 1.3) |
| Lambda retorna 0 críticos | `fraud_summary` não foi gerado pelo silver | Adicionar `salvar_fraud_summary()` (Parte 3) |
| Conexão `aws_default` não encontrada | Conexão não cadastrada | Parte 7 — cadastrar no Airflow UI |
| `.env` não carregado | Docker Compose não encontra o arquivo | Confirmar que `.env` está na mesma pasta do `docker-compose.yml` |
| `silver_transform` falha com tabela bronze vazia | Ingest não rodou antes | Executar `bronze_ingest` primeiro ou rodar o pipeline completo |