# Landing e Bronze

## Estrutura no Unity Catalog

O notebook `00_setup` cria (de forma idempotente):

```
workspace
├── landing          schema
│   └── dados        volume (arquivos)
├── bronze           schema (tabelas Delta)
├── silver           schema (tabelas Delta)
└── gold             schema (tabelas Delta)
```

O "schema LANDING/DADOS" do enunciado é o **volume** `dados` dentro do schema `landing`: volumes guardam arquivos (CSV), enquanto schemas comuns guardam tabelas.

## 01 · Extração para a landing

- Descobre as tabelas em `information_schema.tables` (schema `public`): nenhuma tabela é fixada no código.
- Exporta cada uma com `COPY (SELECT * FROM tabela) TO STDOUT WITH (FORMAT csv, HEADER true)`, o exportador nativo do PostgreSQL.
- Grava em `/Volumes/workspace/landing/dados/<tabela>/<tabela>.csv`, substituindo a pasta a cada execução (carga full).
- A URI vem do secret `seguradora/pg_uri`.

```python
with conexao.cursor() as cur, open(arquivo, "w", encoding="utf-8", newline="") as f:
    cur.copy_expert(f'COPY (SELECT * FROM public."{nome}") TO STDOUT WITH (FORMAT csv, HEADER true)', f)
```

## 02 · Ingestão na bronze

| Decisão | Motivo |
|---|---|
| `inferSchema = false` (tudo `STRING`) | bronze é cópia fiel; no trabalho anterior o `inferSchema` transformou CPF e chassi em `bigint` |
| `escape = '"'` e `multiLine = true` | o PostgreSQL escapa aspas duplicando-as |
| `_ingestion_ts` | quando o registro entrou no lakehouse |
| `_source_file` (via `_metadata.file_path`) | rastreabilidade até o arquivo de origem |
| `_batch_id` (UUID) | identifica a execução |
| `mode("overwrite")` + Delta | cada execução cria uma nova versão; o histórico permite *time travel* |

```python
spark.read.format("csv").option("header", True).option("inferSchema", False) \
    .option("escape", '"').load(f"{LANDING_PATH}/{nome}/") \
    .withColumn("_ingestion_ts", F.current_timestamp()) \
    .withColumn("_source_file", F.col("_metadata.file_path")) \
    .write.format("delta").mode("overwrite").saveAsTable(f"workspace.bronze.{nome}")
```

| Tabela bronze | Linhas |
|---|---|
| apolice | 76 |
| carro | 60 |
| cliente | 40 |
| marca | 9 |
| sinistro | 48 |
