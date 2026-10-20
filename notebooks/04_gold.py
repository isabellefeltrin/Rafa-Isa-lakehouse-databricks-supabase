# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Modelagem dimensional: SILVER → GOLD (Ralph Kimball)
# MAGIC
# MAGIC Constrói um **star schema** (modelo estrela) para analisar prêmios e sinistros da seguradora.
# MAGIC
# MAGIC ```
# MAGIC                 dim_tempo
# MAGIC                     │
# MAGIC  dim_cliente ── fato_apolice ── dim_veiculo
# MAGIC        │                            │
# MAGIC        └────── fato_sinistro ───────┘
# MAGIC                 │        │
# MAGIC          dim_tempo   dim_tipo_sinistro
# MAGIC ```
# MAGIC
# MAGIC **Processos de negócio e grão**
# MAGIC
# MAGIC | Fato | Grão (1 linha =) | Métricas |
# MAGIC |---|---|---|
# MAGIC | `fato_apolice` | uma apólice emitida | `valor_premio`, `vigencia_dias` |
# MAGIC | `fato_sinistro` | um sinistro registrado | `valor_sinistro`, `qtd_sinistro` |
# MAGIC
# MAGIC **Decisões de modelagem**
# MAGIC
# MAGIC - **Chaves substitutas** (`sk_*`) inteiras e sem significado; a chave natural fica como atributo.
# MAGIC - **Membro desconhecido** (`sk = -1`) em cada dimensão: um fato nunca fica sem dimensão, mesmo quando o
# MAGIC   cliente ou carro foi rejeitado na silver.
# MAGIC - `marca` é **desnormalizada** dentro de `dim_veiculo` (Kimball evita snowflake).
# MAGIC - `dim_tempo` usa chave inteligente `AAAAMMDD`, prática comum para datas.
# MAGIC - Dimensões **SCD Tipo 1** (sobrescreve): a carga é full a cada execução do Job.
# MAGIC - Dimensões conformadas (`dim_cliente`, `dim_veiculo`, `dim_tempo`) são compartilhadas pelas duas fatos,
# MAGIC   permitindo *drill-across* (ex.: sinistralidade = sinistros ÷ prêmios por marca).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def silver(nome: str) -> DataFrame:
    return spark.table(tabela(SCHEMA_SILVER, nome))


def gravar(df: DataFrame, nome: str, comentario: str) -> DataFrame:
    destino = tabela(SCHEMA_GOLD, nome)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(destino)
    spark.sql(f"COMMENT ON TABLE {destino} IS '{comentario}'")
    print(f"{destino:<35} {spark.table(destino).count():>5} linhas")
    return spark.table(destino)


def com_sk(df: DataFrame, sk: str, chave_natural: str) -> DataFrame:
    """Gera chave substituta sequencial (1..n) ordenada pela chave natural."""
    return df.withColumn(sk, F.row_number().over(Window.orderBy(chave_natural))).select(sk, *df.columns)


def membro_desconhecido(df: DataFrame, sk: str) -> DataFrame:
    """Linha sk = -1 com 'Desconhecido' nos textos e NULL nos demais campos."""
    valores = []
    for campo in df.schema.fields:
        if campo.name == sk:
            valores.append(F.lit(-1).alias(sk))
        elif campo.dataType.simpleString() == "string":
            valores.append(F.lit("Desconhecido").alias(campo.name))
        else:
            valores.append(F.lit(None).cast(campo.dataType).alias(campo.name))
    return df.sparkSession.range(1).select(*valores)

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_tempo
# MAGIC
# MAGIC Calendário contínuo que cobre, em anos completos, todas as datas de apólices e sinistros.

# COMMAND ----------

datas = (silver("apolice").select(F.col("data_inicio").alias("d"))
         .union(silver("apolice").select("data_fim"))
         .union(silver("sinistro").select("data_ocorrencia")))
limites = datas.agg(F.min("d").alias("ini"), F.max("d").alias("fim")).first()

NOMES_MES = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto",
             "Setembro", "Outubro", "Novembro", "Dezembro"]
NOMES_DIA = ["Domingo", "Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado"]  # dayofweek: 1 = domingo

dim_tempo = (
    spark.sql(f"""
        SELECT explode(sequence(DATE'{limites.ini.year}-01-01', DATE'{limites.fim.year}-12-31', INTERVAL 1 DAY)) AS data
    """)
    .select(
        F.date_format("data", "yyyyMMdd").cast("int").alias("sk_tempo"),
        "data",
        F.year("data").alias("ano"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        F.element_at(F.array(*[F.lit(m) for m in NOMES_MES]), F.month("data")).alias("nome_mes"),
        F.date_format("data", "yyyy-MM").alias("ano_mes"),
        F.dayofmonth("data").alias("dia"),
        F.dayofweek("data").alias("dia_semana"),
        F.element_at(F.array(*[F.lit(d) for d in NOMES_DIA]), F.dayofweek("data")).alias("nome_dia_semana"),
        F.dayofweek("data").isin(1, 7).alias("fim_de_semana"),
    )
)
dim_tempo = gravar(dim_tempo, "dim_tempo", "Dimensão calendário (chave AAAAMMDD)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_cliente

# COMMAND ----------

idade = F.floor(F.months_between(F.current_date(), F.col("data_nascimento")) / 12).cast("int")
dim_cliente = com_sk(
    silver("cliente").select(
        F.col("id").alias("id_cliente"),
        "nome",
        F.concat_ws(".", F.substring("cpf", 1, 3), F.lit("***"), F.lit("***")).alias("cpf_mascarado"),  # LGPD
        "email", "cidade", "uf", "data_nascimento",
        F.when(idade < 25, "18-24").when(idade < 35, "25-34").when(idade < 45, "35-44")
         .when(idade < 60, "45-59").otherwise("60+").alias("faixa_etaria"),
    ),
    "sk_cliente", "id_cliente",
)
dim_cliente = gravar(dim_cliente.unionByName(membro_desconhecido(dim_cliente, "sk_cliente")),
                     "dim_cliente", "Dimensão cliente (SCD1, CPF mascarado)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_veiculo
# MAGIC
# MAGIC Une `carro` + `marca` (desnormalização) e deriva a faixa de idade do veículo.

# COMMAND ----------

idade_veiculo = F.year(F.current_date()) - F.col("ano")
dim_veiculo = com_sk(
    silver("carro").alias("c")
    .join(silver("marca").alias("m"), F.col("c.marca_id") == F.col("m.id"), "left")
    .select(
        "c.placa", "c.modelo", "c.chassi",
        F.coalesce(F.col("m.nome"), F.lit("Desconhecida")).alias("marca"),
        F.col("c.ano").alias("ano_fabricacao"),
        "c.cor",
        F.when(idade_veiculo <= 3, "0-3 anos").when(idade_veiculo <= 8, "4-8 anos")
         .otherwise("9+ anos").alias("faixa_idade_veiculo"),
    ),
    "sk_veiculo", "placa",
)
dim_veiculo = gravar(dim_veiculo.unionByName(membro_desconhecido(dim_veiculo, "sk_veiculo")),
                     "dim_veiculo", "Dimensão veículo (carro + marca desnormalizada)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_tipo_sinistro

# COMMAND ----------

CATEGORIAS = {
    "COLISAO": "Dano parcial", "VIDROS": "Dano parcial", "TERCEIROS": "Responsabilidade civil",
    "ROUBO": "Perda total", "FURTO": "Perda total", "INCENDIO": "Perda total", "ALAGAMENTO": "Evento natural",
}
dim_tipo = com_sk(
    spark.createDataFrame(list(CATEGORIAS.items()), "tipo STRING, categoria STRING"),
    "sk_tipo_sinistro", "tipo",
)
dim_tipo = gravar(dim_tipo.unionByName(membro_desconhecido(dim_tipo, "sk_tipo_sinistro")),
                  "dim_tipo_sinistro", "Dimensão tipo de sinistro")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_apolice
# MAGIC
# MAGIC Busca de chaves (*lookup*): `apolice.placa → dim_veiculo` e `carro.cliente_id → dim_cliente`.
# MAGIC Chaves não encontradas recebem `-1`.

# COMMAND ----------

carro_dono = silver("carro").select("placa", "cliente_id")
lk_cli = dim_cliente.filter("sk_cliente > 0").select("sk_cliente", "id_cliente")
lk_vei = dim_veiculo.filter("sk_veiculo > 0").select("sk_veiculo", "placa")

base_apolice = (
    silver("apolice").alias("a")
    .join(carro_dono.alias("c"), "placa", "left")
    .join(lk_vei, "placa", "left")
    .join(lk_cli, F.col("c.cliente_id") == lk_cli.id_cliente, "left")
)
fato_apolice = base_apolice.select(
    F.col("a.id").alias("id_apolice"),  # dimensão degenerada
    F.coalesce("sk_cliente", F.lit(-1)).alias("sk_cliente"),
    F.coalesce("sk_veiculo", F.lit(-1)).alias("sk_veiculo"),
    F.date_format("a.data_inicio", "yyyyMMdd").cast("int").alias("sk_tempo_inicio"),
    F.date_format("a.data_fim", "yyyyMMdd").cast("int").alias("sk_tempo_fim"),
    F.col("a.valor").alias("valor_premio"),
    F.datediff("a.data_fim", "a.data_inicio").alias("vigencia_dias"),
)
fato_apolice = gravar(fato_apolice, "fato_apolice", "Fato apólice (grão: 1 apólice)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_sinistro

# COMMAND ----------

lk_tipo = dim_tipo.filter("sk_tipo_sinistro > 0").select("sk_tipo_sinistro", "tipo")
fato_sinistro = (
    silver("sinistro").alias("s")
    .join(fato_apolice.select("id_apolice", "sk_cliente", "sk_veiculo"),
          F.col("s.apolice_id") == F.col("id_apolice"), "left")
    .join(lk_tipo, "tipo", "left")
    .select(
        F.col("s.id").alias("id_sinistro"),
        F.col("s.apolice_id").alias("id_apolice"),  # dimensão degenerada
        F.coalesce("sk_cliente", F.lit(-1)).alias("sk_cliente"),
        F.coalesce("sk_veiculo", F.lit(-1)).alias("sk_veiculo"),
        F.date_format("s.data_ocorrencia", "yyyyMMdd").cast("int").alias("sk_tempo"),
        F.coalesce("sk_tipo_sinistro", F.lit(-1)).alias("sk_tipo_sinistro"),
        F.col("s.valor").alias("valor_sinistro"),
        F.lit(1).alias("qtd_sinistro"),
    )
)
fato_sinistro = gravar(fato_sinistro, "fato_sinistro", "Fato sinistro (grão: 1 sinistro)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validações do modelo
# MAGIC
# MAGIC Garantem que a gold está íntegra antes do Job terminar com sucesso.

# COMMAND ----------

checks = {
    "fato_apolice com as mesmas linhas da silver": fato_apolice.count() == silver("apolice").count(),
    "fato_sinistro com as mesmas linhas da silver": fato_sinistro.count() == silver("sinistro").count(),
    "soma de prêmios igual à silver":
        fato_apolice.agg(F.sum("valor_premio")).first()[0] == silver("apolice").agg(F.sum("valor")).first()[0],
    "toda sk_tempo da fato existe em dim_tempo":
        fato_sinistro.join(dim_tempo, "sk_tempo", "left_anti").count() == 0,
    "sk_cliente única em dim_cliente":
        dim_cliente.groupBy("sk_cliente").count().filter("count > 1").count() == 0,
}
for nome, ok in checks.items():
    print(("OK   " if ok else "FALHA"), nome)
assert all(checks.values()), "Validação da camada gold falhou"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas de exemplo
# MAGIC
# MAGIC ### Sinistralidade por marca (*drill-across* entre as duas fatos)

# COMMAND ----------

display(spark.sql(f"""
    WITH premios AS (
        SELECT v.marca, SUM(f.valor_premio) AS total_premios
        FROM {tabela(SCHEMA_GOLD, 'fato_apolice')} f
        JOIN {tabela(SCHEMA_GOLD, 'dim_veiculo')} v USING (sk_veiculo)
        GROUP BY v.marca
    ),
    sinistros AS (
        SELECT v.marca, SUM(f.valor_sinistro) AS total_sinistros, SUM(f.qtd_sinistro) AS qtd
        FROM {tabela(SCHEMA_GOLD, 'fato_sinistro')} f
        JOIN {tabela(SCHEMA_GOLD, 'dim_veiculo')} v USING (sk_veiculo)
        GROUP BY v.marca
    )
    SELECT p.marca, p.total_premios, COALESCE(s.total_sinistros, 0) AS total_sinistros,
           COALESCE(s.qtd, 0) AS qtd_sinistros,
           ROUND(COALESCE(s.total_sinistros, 0) / p.total_premios * 100, 1) AS sinistralidade_pct
    FROM premios p LEFT JOIN sinistros s USING (marca)
    ORDER BY sinistralidade_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sinistros por categoria e trimestre

# COMMAND ----------

display(spark.sql(f"""
    SELECT t.ano, t.trimestre, d.categoria, COUNT(*) AS qtd, SUM(f.valor_sinistro) AS valor_total
    FROM {tabela(SCHEMA_GOLD, 'fato_sinistro')} f
    JOIN {tabela(SCHEMA_GOLD, 'dim_tempo')} t USING (sk_tempo)
    JOIN {tabela(SCHEMA_GOLD, 'dim_tipo_sinistro')} d USING (sk_tipo_sinistro)
    GROUP BY ALL
    ORDER BY t.ano, t.trimestre, d.categoria
"""))
