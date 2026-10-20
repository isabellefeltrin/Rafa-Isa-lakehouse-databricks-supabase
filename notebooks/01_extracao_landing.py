# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Extração: Supabase (PostgreSQL) → LANDING/DADOS (CSV)
# MAGIC
# MAGIC Extrai **todas as tabelas** do schema `public` do banco `seguradora` no Supabase e grava cada uma como CSV
# MAGIC no volume `landing.dados`:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/workspace/landing/dados/
# MAGIC ├── apolice/apolice.csv
# MAGIC ├── carro/carro.csv
# MAGIC ├── cliente/cliente.csv
# MAGIC ├── marca/marca.csv
# MAGIC └── sinistro/sinistro.csv
# MAGIC ```
# MAGIC
# MAGIC - As tabelas são descobertas dinamicamente no `information_schema`: uma tabela nova na origem entra no
# MAGIC   pipeline sem mudar código.
# MAGIC - A exportação usa o `COPY ... TO STDOUT WITH CSV HEADER` do próprio PostgreSQL, que gera um CSV fiel
# MAGIC   (aspas, vírgulas e acentos corretos).
# MAGIC - A credencial vem de um **secret** do Databricks; nada de senha no código.
# MAGIC
# MAGIC ### Modos de extração (parâmetro `modo_extracao`)
# MAGIC
# MAGIC | Modo | Quando usar |
# MAGIC |---|---|
# MAGIC | `supabase` (padrão) | o Databricks consegue abrir conexão com o Supabase |
# MAGIC | `upload_externo` | o Free Edition bloqueou a saída de rede para o Supabase. Os CSVs são enviados ao volume pelo script `scripts/extrair_supabase_para_volume.py` e este notebook só **valida** a landing |

# COMMAND ----------

# MAGIC %pip install psycopg2-binary==2.9.10 --quiet

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import os
from datetime import datetime, timezone

dbutils.widgets.dropdown("modo_extracao", "supabase", ["supabase", "upload_externo"], "Modo de extração")
dbutils.widgets.text("pg_uri", "", "URI PostgreSQL (fallback)")
MODO = dbutils.widgets.get("modo_extracao")
print(f"Modo de extração: {MODO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extração direta do Supabase
# MAGIC
# MAGIC A URI fica no secret `seguradora/pg_uri`. Use a URI do **Session pooler** do Supabase (IPv4); a
# MAGIC "Direct connection" é só IPv6. Cada execução **substitui** a pasta da tabela (carga full).

# COMMAND ----------

def obter_uri() -> str:
    try:
        return dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
    except Exception:
        uri = dbutils.widgets.get("pg_uri")
        if not uri:
            raise ValueError("URI não encontrada: crie o secret 'seguradora/pg_uri' (ver README) ou preencha o widget 'pg_uri'.")
        return uri


def extrair_do_supabase() -> list[tuple]:
    import psycopg2

    try:
        conexao = psycopg2.connect(obter_uri(), connect_timeout=20)
    except psycopg2.OperationalError as erro:
        raise RuntimeError(
            "Não foi possível conectar ao Supabase. No Databricks Free Edition a saída de rede é limitada a "
            "domínios confiáveis. Use o modo 'upload_externo' (ver README, seção 'Plano B').\n" + str(erro)
        ) from erro

    conexao.set_client_encoding("UTF8")
    with conexao.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tabelas = [linha[0] for linha in cur.fetchall()]
    print(f"{len(tabelas)} tabelas encontradas: {tabelas}")

    resumo = []
    for nome in tabelas:
        pasta = f"{LANDING_PATH}/{nome}"
        arquivo = f"{pasta}/{nome}.csv"
        dbutils.fs.rm(pasta, True)
        os.makedirs(pasta, exist_ok=True)
        with conexao.cursor() as cur, open(arquivo, "w", encoding="utf-8", newline="") as f:
            cur.copy_expert(f'COPY (SELECT * FROM public."{nome}") TO STDOUT WITH (FORMAT csv, HEADER true)', f)
            cur.execute(f'SELECT COUNT(*) FROM public."{nome}"')
            linhas = cur.fetchone()[0]
        resumo.append((nome, linhas, arquivo))
        print(f"{nome:<10} {linhas:>5} linhas -> {arquivo}")
    conexao.close()
    return resumo


if MODO == "supabase":
    resumo = extrair_do_supabase()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação da landing
# MAGIC
# MAGIC Nos dois modos, confere se cada tabela tem seu CSV no volume. Falha o Job se a landing estiver vazia.

# COMMAND ----------

pastas = sorted(f.name.strip("/") for f in dbutils.fs.ls(LANDING_PATH) if f.name.endswith("/"))
assert pastas, f"Landing vazia: nenhum CSV em {LANDING_PATH}"

if MODO == "upload_externo":
    resumo = []
    for nome in pastas:
        arquivo = f"{LANDING_PATH}/{nome}/{nome}.csv"
        with open(arquivo, encoding="utf-8") as f:
            linhas = sum(1 for _ in f) - 1  # desconta o cabeçalho (o volume de dados não tem quebras de linha nos campos)
        resumo.append((nome, linhas, arquivo))

display(spark.createDataFrame(resumo, "tabela STRING, linhas_origem LONG, arquivo STRING"))

# COMMAND ----------

dbutils.notebook.exit(f"{len(resumo)} tabelas na landing ({MODO}) em {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC")
