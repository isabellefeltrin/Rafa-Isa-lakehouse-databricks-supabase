dbutils.widgets.dropdown("escopo", "tudo", ["tudo", "landing", "bronze", "silver", "gold"], "Escopo")
dbutils.widgets.text("confirmar", "", "Digite SIM para apagar")

ESCOPO = dbutils.widgets.get("escopo")
CONFIRMADO = dbutils.widgets.get("confirmar").strip().upper() == "SIM"

ordem = [SCHEMA_LANDING, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD]
alvos = ordem if ESCOPO == "tudo" else [ESCOPO]


existentes = {linha[0] for linha in spark.sql(f"SHOW SCHEMAS IN {CATALOGO}").collect()}

for schema in alvos:
    if schema not in existentes:
        print(f"{CATALOGO}.{schema}: não existe")
        continue
    tabelas = [t.tableName for t in spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect()]
    extra = f" + volume '{VOLUME_LANDING}' com os CSVs" if schema == SCHEMA_LANDING else ""
    print(f"{CATALOGO}.{schema}: {len(tabelas)} tabela(s) {tabelas}{extra}")



if not CONFIRMADO:
    print(f"Modo simulação: nada foi apagado. Para apagar '{ESCOPO}', preencha o widget 'confirmar' com SIM.")
else:
    for schema in alvos:
        if schema in existentes:
            spark.sql(f"DROP SCHEMA IF EXISTS {CATALOGO}.{schema} CASCADE")
            print(f"apagado: {CATALOGO}.{schema}")
    dbutils.widgets.remove("confirmar")  # evita reexecução acidental com a trava aberta
    print("Limpeza concluída. Rode o Job (ou o 00_setup) para recriar a estrutura.")
