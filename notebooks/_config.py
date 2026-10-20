# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração compartilhada
# MAGIC
# MAGIC Incluído pelos demais notebooks com `%run ./_config`. Centraliza nomes de catálogo, schemas e caminhos
# MAGIC para que nenhum notebook tenha valores fixos espalhados.
# MAGIC
# MAGIC | Parâmetro | Padrão | Descrição |
# MAGIC |---|---|---|
# MAGIC | `catalogo` | `workspace` | Catálogo do Unity Catalog (no Free Edition o padrão é `workspace`) |

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")

SCHEMA_LANDING = "landing"
VOLUME_LANDING = "dados"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

# Pasta do volume LANDING/DADOS, onde ficam os CSVs extraídos do PostgreSQL
LANDING_PATH = f"/Volumes/{CATALOGO}/{SCHEMA_LANDING}/{VOLUME_LANDING}"

# Escopo de secrets com a URI de conexão do PostgreSQL (ver README)
SECRET_SCOPE = "seguradora"
SECRET_KEY = "pg_uri"


def tabela(schema: str, nome: str) -> str:
    """Nome completo de uma tabela: catalogo.schema.nome"""
    return f"{CATALOGO}.{schema}.{nome}"


print(f"Catálogo: {CATALOGO} | Landing: {LANDING_PATH}")
