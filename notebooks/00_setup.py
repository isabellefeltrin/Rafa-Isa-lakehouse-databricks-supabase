# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do Lakehouse
# MAGIC
# MAGIC Cria a estrutura da arquitetura medalhão no Unity Catalog. É idempotente (`IF NOT EXISTS`), então
# MAGIC roda como primeira task do Job sem risco.
# MAGIC
# MAGIC ```
# MAGIC workspace
# MAGIC ├── landing   → volume "dados" (arquivos CSV brutos)
# MAGIC ├── bronze    → tabelas Delta, cópia fiel da landing
# MAGIC ├── silver    → tabelas Delta tipadas e validadas (Data Quality)
# MAGIC └── gold      → modelo dimensional (Ralph Kimball)
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

comentarios = {
    SCHEMA_LANDING: "Camada LANDING: arquivos extraídos da origem (PostgreSQL) em CSV",
    SCHEMA_BRONZE: "Camada BRONZE: dados brutos em Delta Lake, sem transformação",
    SCHEMA_SILVER: "Camada SILVER: dados tipados, limpos e validados",
    SCHEMA_GOLD: "Camada GOLD: modelo dimensional (Kimball) para análise",
}

for schema, comentario in comentarios.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema} COMMENT '{comentario}'")
    print(f"schema ok: {CATALOGO}.{schema}")

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA_LANDING}.{VOLUME_LANDING} "
    "COMMENT 'Arquivos CSV extraídos do banco seguradora'"
)
print(f"volume ok: {LANDING_PATH}")
