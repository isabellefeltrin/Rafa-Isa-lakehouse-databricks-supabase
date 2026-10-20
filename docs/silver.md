# Silver: Data Quality

O notebook `03_silver` transforma a bronze (tudo texto) em dados **tipados e confiáveis**. Cada regra é de um de dois tipos:

- **Correção:** o valor é ajustado e o registro segue (ex.: CPF `123.456.789-09` → `12345678909`).
- **Rejeição:** o registro não entra na silver e vai para `silver.dq_quarentena`, com o motivo.

## Como as regras são aplicadas

Uma função genérica recebe um dicionário `{motivo: condição que o registro válido satisfaz}`. Um registro pode falhar em mais de uma regra, e todos os motivos ficam registrados:

```python
def aplicar_regras(df, regras):
    motivos = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(motivo)) for motivo, cond in regras.items()]
    df = df.withColumn("_dq_motivo", F.concat_ws("; ", *motivos))
    return df.filter("_dq_motivo = ''"), df.filter("_dq_motivo != ''")
```

Condição nula conta como falha (ex.: `valor > 0` com valor nulo).

## Regras por tabela

### marca
| Regra | Tipo |
|---|---|
| trim e maiúsculas no nome | correção |
| `id` e `nome` obrigatórios | rejeição |
| `id` único | rejeição (duplicado) |

### cliente
| Regra | Tipo |
|---|---|
| nome com espaços colapsados e Inicial Maiúscula | correção |
| CPF só com dígitos | correção |
| cidade padronizada; nula → `Não informada`; UF em maiúsculas | correção |
| e-mail em minúsculas; formato inválido → `NULL` | correção |
| CPF com 11 dígitos, não repetidos e **dígitos verificadores válidos** (módulo 11) | rejeição |
| UF com 2 letras | rejeição |
| nascimento não futuro e idade ≥ 18 | rejeição |

A validação do CPF é feita em Spark SQL puro (`aggregate` + `sequence`), sem UDF Python, o que é mais rápido e compatível com serverless:

```python
SOMA_DV1 = "aggregate(sequence(0, 8), 0, (acc, i) -> acc + cast(substring(cpf, i + 1, 1) AS INT) * (10 - i))"
DV = lambda soma: F.expr(f"CASE WHEN ({soma}) % 11 < 2 THEN 0 ELSE 11 - ({soma}) % 11 END")
```

### carro
| Regra | Tipo |
|---|---|
| placa maiúscula sem hífen/espaço; modelo e cor padronizados; cor nula → `NAO INFORMADA` | correção |
| placa no padrão antigo `ABC1234` ou Mercosul `ABC1D23` | rejeição |
| ano entre 1950 e o ano atual + 1 | rejeição |

### apolice
| Regra | Tipo |
|---|---|
| `valor` → `DECIMAL(10,2)`, datas → `DATE` | correção |
| prêmio > 0 | rejeição |
| `data_fim` > `data_inicio` | rejeição |
| `data_inicio` não futura | rejeição |

### sinistro
| Regra | Tipo |
|---|---|
| tipo sem acento, com trim e maiúsculo (`Colisão` → `COLISAO`) | correção |
| tipo no domínio conhecido | rejeição |
| valor obrigatório e > 0 | rejeição |
| data de ocorrência não futura | rejeição |
| apólice existente e aprovada na silver | rejeição (integridade referencial) |
| ocorrência dentro da vigência da apólice | rejeição |
| mesmo evento (apólice + data + tipo + valor) com outro `id` | rejeição (duplicado) |

## Resultado

| Tabela | Lidos | Aprovados | Rejeitados | % aprovado |
|---|---|---|---|---|
| marca | 9 | 9 | 0 | 100,0 |
| cliente | 40 | 36 | 4 | 90,0 |
| carro | 60 | 58 | 2 | 96,7 |
| apolice | 76 | 73 | 3 | 96,1 |
| sinistro | 48 | 41 | 7 | 85,4 |

## Tabelas de controle

- **`silver.dq_quarentena`**: `tabela`, `motivo`, `registro` (JSON) e `_dq_ts`. Recriada a cada execução.
- **`silver.dq_log`**: métricas por tabela e execução. Acumula histórico (`append`), permitindo acompanhar a qualidade ao longo do tempo.

```sql
SELECT tabela, motivo, registro FROM workspace.silver.dq_quarentena ORDER BY tabela;
```
