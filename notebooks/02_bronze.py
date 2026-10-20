# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingestão: LANDING/DADOS (CSV) → BRONZE (Delta Lake)
# MAGIC
# MAGIC Lê cada pasta do volume `landing.dados` e grava uma tabela Delta no schema `bronze`.
# MAGIC
# MAGIC **Regras da camada bronze**
# MAGIC
# MAGIC - Cópia **fiel** da origem: nenhuma limpeza ou regra de negócio aqui.
# MAGIC - Todas as colunas são gravadas como `STRING` (`inferSchema = false`). No trabalho anterior o
# MAGIC   `inferSchema` transformou `cpf` e `chassi` em `bigint`, o que apaga zeros à esquerda; a tipagem
# MAGIC   correta agora é responsabilidade da silver.
# MAGIC - Colunas de auditoria: `_ingestion_ts` (quando entrou), `_source_file` (de qual arquivo veio)
# MAGIC   e `_batch_id` (identificador da execução).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import uuid
from pyspark.sql import functions as F

BATCH_ID = str(uuid.uuid4())

tabelas = sorted(f.name.strip("/") for f in dbutils.fs.ls(LANDING_PATH) if f.name.endswith("/"))
print(f"Batch {BATCH_ID}: {tabelas}")

# COMMAND ----------

resumo = []
for nome in tabelas:
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)  # tudo STRING na bronze
        .option("multiLine", True)
        .option("escape", '"')         # o COPY do PostgreSQL escapa aspas duplicando-as
        .option("encoding", "UTF-8")
        .load(f"{LANDING_PATH}/{nome}/")
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )

    destino = tabela(SCHEMA_BRONZE, nome)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(destino)
    )
    spark.sql(f"COMMENT ON TABLE {destino} IS 'Bronze: cópia bruta de public.{nome} (PostgreSQL)'")

    linhas = spark.table(destino).count()
    resumo.append((nome, linhas))
    print(f"{destino:<35} {linhas:>5} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

display(spark.createDataFrame(resumo, "tabela STRING, linhas_bronze LONG"))

# COMMAND ----------

display(spark.table(tabela(SCHEMA_BRONZE, "cliente")).limit(10))

# COMMAND ----------

# Histórico Delta: cada execução do Job gera uma nova versão (time travel disponível)
display(spark.sql(f"DESCRIBE HISTORY {tabela(SCHEMA_BRONZE, 'carro')}").select("version", "timestamp", "operation"))
