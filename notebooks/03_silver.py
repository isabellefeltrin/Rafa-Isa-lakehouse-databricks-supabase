# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Data Quality: BRONZE → SILVER
# MAGIC
# MAGIC Lê as tabelas da bronze, aplica **tipagem, padronização, deduplicação e validações de regra de negócio**
# MAGIC e grava o resultado confiável no schema `silver`.
# MAGIC
# MAGIC Cada registro passa por dois tipos de tratamento:
# MAGIC
# MAGIC | Tipo | O que acontece | Exemplo |
# MAGIC |---|---|---|
# MAGIC | **Correção** | o valor é ajustado e o registro segue | CPF `123.456.789-09` → `12345678909` |
# MAGIC | **Rejeição** | o registro não entra na silver e vai para a quarentena com o motivo | prêmio negativo |
# MAGIC
# MAGIC Saídas:
# MAGIC
# MAGIC - `silver.<tabela>`: dados aprovados, com tipos corretos.
# MAGIC - `silver.dq_quarentena`: registros rejeitados (tabela, motivo e o registro original em JSON).
# MAGIC - `silver.dq_log`: métricas de cada execução (lidos, aprovados, rejeitados), com histórico.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

execucao_ts = spark.sql("SELECT current_timestamp() AS ts").first().ts
quarentenas: list[DataFrame] = []
metricas: list[tuple] = []


def bronze(nome: str) -> DataFrame:
    return spark.table(tabela(SCHEMA_BRONZE, nome))


def texto(col: str):
    """trim + espaços internos colapsados; string vazia vira NULL."""
    limpo = F.regexp_replace(F.trim(F.col(col)), r"\s+", " ")
    return F.when(limpo == "", None).otherwise(limpo)


def sem_acento(c):
    return F.translate(c, "ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇáàãâäéèêëíìîïóòõôöúùûüç",
                          "AAAAAEEEEIIIIOOOOOUUUUCaaaaaeeeeiiiiooooouuuuc")


def deduplicar(df: DataFrame, chave: list[str], ordem) -> tuple[DataFrame, DataFrame]:
    """Mantém o primeiro registro de cada chave; os demais vão para a quarentena."""
    w = Window.partitionBy(*chave).orderBy(ordem)
    df = df.withColumn("_rn", F.row_number().over(w))
    return df.filter("_rn = 1").drop("_rn"), df.filter("_rn > 1").drop("_rn").withColumn("_dq_motivo", F.lit("duplicado"))


def aplicar_regras(df: DataFrame, regras: dict) -> tuple[DataFrame, DataFrame]:
    """regras = {motivo: condição que o registro VÁLIDO satisfaz}. Condição nula conta como falha."""
    motivos = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(motivo)) for motivo, cond in regras.items()]
    df = df.withColumn("_dq_motivo", F.concat_ws("; ", *motivos))
    return df.filter("_dq_motivo = ''").drop("_dq_motivo"), df.filter("_dq_motivo != ''")


def gravar(nome: str, lidos: int, aprovados: DataFrame, rejeitados: list[DataFrame], colunas: list[str]) -> DataFrame:
    destino = tabela(SCHEMA_SILVER, nome)
    final = aprovados.select(*colunas, F.lit(execucao_ts).alias("_silver_ts"))
    final.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(destino)

    for r in rejeitados:
        orig = [c for c in r.columns if not c.startswith("_")]
        quarentenas.append(r.select(
            F.lit(nome).alias("tabela"),
            F.col("_dq_motivo").alias("motivo"),
            F.to_json(F.struct(*orig)).alias("registro"),
            F.lit(execucao_ts).alias("_dq_ts"),
        ))

    n_ok = spark.table(destino).count()
    metricas.append((nome, lidos, n_ok, lidos - n_ok))
    print(f"{destino:<30} lidos={lidos:<4} aprovados={n_ok:<4} rejeitados={lidos - n_ok}")
    return spark.table(destino)

# COMMAND ----------

# MAGIC %md
# MAGIC ## marca
# MAGIC
# MAGIC | Regra | Tipo |
# MAGIC |---|---|
# MAGIC | `nome` com trim e em maiúsculas | correção |
# MAGIC | `id` inteiro e não nulo | rejeição |
# MAGIC | `nome` obrigatório | rejeição |
# MAGIC | `id` único | rejeição (duplicado) |

# COMMAND ----------

b = bronze("marca")
df = b.select(
    F.col("id").cast("int").alias("id"),
    F.upper(texto("nome")).alias("nome"),
    "_ingestion_ts", "_source_file",
)
df, dup = deduplicar(df, ["id"], F.col("_ingestion_ts").desc())
ok, rej = aplicar_regras(df, {
    "id_invalido": F.col("id").isNotNull(),
    "nome_obrigatorio": F.col("nome").isNotNull(),
})
silver_marca = gravar("marca", b.count(), ok, [rej, dup], ["id", "nome"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## cliente
# MAGIC
# MAGIC | Regra | Tipo |
# MAGIC |---|---|
# MAGIC | `nome` com trim, espaços colapsados e Inicial Maiúscula | correção |
# MAGIC | `cpf` só com dígitos (remove `.` e `-`) | correção |
# MAGIC | `cidade` padronizada; nula vira `Não informada` | correção |
# MAGIC | `uf` em maiúsculas | correção |
# MAGIC | `email` em minúsculas; formato inválido vira `NULL` | correção |
# MAGIC | `cpf` com 11 dígitos, sem todos os dígitos iguais e **dígitos verificadores corretos** | rejeição |
# MAGIC | `data_nascimento` não pode ser futura e o cliente deve ter 18 anos ou mais | rejeição |
# MAGIC | `id` único | rejeição (duplicado) |

# COMMAND ----------

# Validação de CPF (módulo 11) em Spark SQL puro, sem UDF: funciona em serverless e é vetorizada
SOMA_DV1 = "aggregate(sequence(0, 8), 0, (acc, i) -> acc + cast(substring(cpf, i + 1, 1) AS INT) * (10 - i))"
SOMA_DV2 = "aggregate(sequence(0, 9), 0, (acc, i) -> acc + cast(substring(cpf, i + 1, 1) AS INT) * (11 - i))"
DV = lambda soma: F.expr(f"CASE WHEN ({soma}) % 11 < 2 THEN 0 ELSE 11 - ({soma}) % 11 END")

b = bronze("cliente")
email = F.lower(texto("email"))
df = b.select(
    F.col("id").cast("int").alias("id"),
    F.initcap(F.lower(texto("nome"))).alias("nome"),
    F.regexp_replace(F.col("cpf"), r"[^0-9]", "").alias("cpf"),
    F.when(email.rlike(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"), email).alias("email"),
    F.coalesce(F.initcap(F.lower(texto("cidade"))), F.lit("Não informada")).alias("cidade"),
    F.upper(texto("uf")).alias("uf"),
    F.to_date("data_nascimento").alias("data_nascimento"),
    "_ingestion_ts", "_source_file",
)
df, dup = deduplicar(df, ["id"], F.col("_ingestion_ts").desc())
cpf_formato = F.col("cpf").rlike(r"^\d{11}$") & ~F.col("cpf").rlike(r"^(\d)\1{10}$")
ok, rej = aplicar_regras(df, {
    "id_invalido": F.col("id").isNotNull(),
    "nome_obrigatorio": F.col("nome").isNotNull(),
    "cpf_formato_invalido": cpf_formato,
    "cpf_digito_verificador": F.when(cpf_formato,
        (DV(SOMA_DV1) == F.substring("cpf", 10, 1).cast("int")) &
        (DV(SOMA_DV2) == F.substring("cpf", 11, 1).cast("int"))).otherwise(F.lit(True)),
    "uf_invalida": F.col("uf").rlike(r"^[A-Z]{2}$"),
    "nascimento_futuro": F.col("data_nascimento") <= F.current_date(),
    "menor_de_idade": F.months_between(F.current_date(), F.col("data_nascimento")) >= 18 * 12,
})
silver_cliente = gravar("cliente", b.count(), ok, [rej, dup],
                        ["id", "nome", "cpf", "email", "cidade", "uf", "data_nascimento"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## carro
# MAGIC
# MAGIC | Regra | Tipo |
# MAGIC |---|---|
# MAGIC | `placa` em maiúsculas, sem hífen e espaços | correção |
# MAGIC | `modelo` e `cor` com trim e em maiúsculas; `cor` nula vira `NAO INFORMADA` | correção |
# MAGIC | `placa` no padrão antigo (`ABC1234`) ou Mercosul (`ABC1D23`) | rejeição |
# MAGIC | `ano` entre 1950 e o ano atual + 1 | rejeição |
# MAGIC | `placa` única | rejeição (duplicado) |

# COMMAND ----------

b = bronze("carro")
df = b.select(
    F.upper(F.regexp_replace(F.col("placa"), r"[\s-]", "")).alias("placa"),
    F.upper(texto("modelo")).alias("modelo"),
    texto("chassi").alias("chassi"),
    F.col("marca_id").cast("int").alias("marca_id"),
    F.col("ano").cast("int").alias("ano"),
    F.coalesce(F.upper(sem_acento(texto("cor"))), F.lit("NAO INFORMADA")).alias("cor"),
    F.col("cliente_id").cast("int").alias("cliente_id"),
    "_ingestion_ts", "_source_file",
)
df, dup = deduplicar(df, ["placa"], F.col("_ingestion_ts").desc())
ok, rej = aplicar_regras(df, {
    "placa_invalida": F.col("placa").rlike(r"^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$"),
    "modelo_obrigatorio": F.col("modelo").isNotNull(),
    "ano_invalido": F.col("ano").between(F.lit(1950), F.year(F.current_date()) + 1),
})
silver_carro = gravar("carro", b.count(), ok, [rej, dup],
                      ["placa", "modelo", "chassi", "marca_id", "ano", "cor", "cliente_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## apolice
# MAGIC
# MAGIC | Regra | Tipo |
# MAGIC |---|---|
# MAGIC | `valor` como `DECIMAL(10,2)`, datas como `DATE` | correção (tipagem) |
# MAGIC | `placa` padronizada como em `carro` | correção |
# MAGIC | `valor` (prêmio) maior que zero | rejeição |
# MAGIC | `data_fim` posterior a `data_inicio` | rejeição |
# MAGIC | `data_inicio` não pode ser futura | rejeição |
# MAGIC | `id` único | rejeição (duplicado) |

# COMMAND ----------

b = bronze("apolice")
df = b.select(
    F.col("id").cast("int").alias("id"),
    F.upper(F.regexp_replace(F.col("placa"), r"[\s-]", "")).alias("placa"),
    F.col("valor").cast("decimal(10,2)").alias("valor"),
    F.to_date("data_inicio").alias("data_inicio"),
    F.to_date("data_fim").alias("data_fim"),
    "_ingestion_ts", "_source_file",
)
df, dup = deduplicar(df, ["id"], F.col("_ingestion_ts").desc())
ok, rej = aplicar_regras(df, {
    "id_invalido": F.col("id").isNotNull(),
    "premio_nao_positivo": F.col("valor") > 0,
    "vigencia_invertida": F.col("data_fim") > F.col("data_inicio"),
    "inicio_futuro": F.col("data_inicio") <= F.current_date(),
})
silver_apolice = gravar("apolice", b.count(), ok, [rej, dup], ["id", "placa", "valor", "data_inicio", "data_fim"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## sinistro
# MAGIC
# MAGIC | Regra | Tipo |
# MAGIC |---|---|
# MAGIC | `tipo` sem acento, com trim e em maiúsculas (`" colisao "`, `Colisão` → `COLISAO`) | correção |
# MAGIC | `tipo` dentro do domínio conhecido | rejeição |
# MAGIC | `valor` obrigatório e maior que zero | rejeição |
# MAGIC | `data_ocorrencia` não pode ser futura | rejeição |
# MAGIC | apólice existente e aprovada na silver | rejeição |
# MAGIC | `data_ocorrencia` dentro da vigência da apólice | rejeição |
# MAGIC | mesmo evento (apólice + data + tipo + valor) registrado com outro `id` | rejeição (duplicado) |

# COMMAND ----------

TIPOS_SINISTRO = ["COLISAO", "ROUBO", "FURTO", "VIDROS", "INCENDIO", "ALAGAMENTO", "TERCEIROS"]

b = bronze("sinistro")
df = b.select(
    F.col("id").cast("int").alias("id"),
    F.col("apolice_id").cast("int").alias("apolice_id"),
    F.to_date("data_ocorrencia").alias("data_ocorrencia"),
    F.upper(sem_acento(texto("tipo"))).alias("tipo"),
    F.col("valor").cast("decimal(12,2)").alias("valor"),
    "_ingestion_ts", "_source_file",
)
df, dup_id = deduplicar(df, ["id"], F.col("_ingestion_ts").desc())
df, dup_evento = deduplicar(df, ["apolice_id", "data_ocorrencia", "tipo", "valor"], F.col("id"))

vigencias = silver_apolice.select(F.col("id").alias("_ap_id"), F.col("data_inicio").alias("_ap_ini"),
                                  F.col("data_fim").alias("_ap_fim"))
df = df.join(vigencias, df.apolice_id == vigencias._ap_id, "left")

ok, rej = aplicar_regras(df, {
    "tipo_desconhecido": F.col("tipo").isin(TIPOS_SINISTRO),
    "valor_invalido": F.col("valor") > 0,
    "ocorrencia_futura": F.col("data_ocorrencia") <= F.current_date(),
    "apolice_inexistente_ou_rejeitada": F.col("_ap_id").isNotNull(),
    "fora_da_vigencia": F.when(F.col("_ap_id").isNull(), F.lit(True))
                         .otherwise(F.col("data_ocorrencia").between(F.col("_ap_ini"), F.col("_ap_fim"))),
})
rej = rej.drop("_ap_id", "_ap_ini", "_ap_fim")
silver_sinistro = gravar("sinistro", b.count(), ok, [rej, dup_id, dup_evento],
                         ["id", "apolice_id", "data_ocorrencia", "tipo", "valor"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarentena e log de qualidade

# COMMAND ----------

quarentena = reduce(DataFrame.unionByName, quarentenas)
(quarentena.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable(tabela(SCHEMA_SILVER, "dq_quarentena")))

log = spark.createDataFrame(metricas, "tabela STRING, lidos LONG, aprovados LONG, rejeitados LONG") \
    .withColumn("pct_aprovado", F.round(F.col("aprovados") / F.col("lidos") * 100, 1)) \
    .withColumn("execucao_ts", F.lit(execucao_ts))
log.write.format("delta").mode("append").saveAsTable(tabela(SCHEMA_SILVER, "dq_log"))

display(log)

# COMMAND ----------

display(spark.table(tabela(SCHEMA_SILVER, "dq_quarentena")).orderBy("tabela", "motivo"))
