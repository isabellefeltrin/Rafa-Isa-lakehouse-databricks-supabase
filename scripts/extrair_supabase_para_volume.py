"""Plano B da extração: Supabase (PostgreSQL) -> CSV -> volume LANDING/DADOS do Databricks.

Use quando o Databricks Free Edition não conseguir abrir conexão com o Supabase (saída de rede restrita).
O script roda no seu computador, lê todas as tabelas do Supabase e envia os CSVs para o volume pela
API de arquivos do Databricks (conexão de entrada, que o Free Edition permite).

Variáveis de ambiente (arquivo .env, ver .env.example):
    PG_URI             URI do Session pooler do Supabase
    DATABRICKS_HOST    https://<seu-workspace>.cloud.databricks.com
    DATABRICKS_TOKEN   token pessoal (Settings > Developer > Access tokens)

Uso:
    uv run python scripts/extrair_supabase_para_volume.py
    uv run python scripts/extrair_supabase_para_volume.py --catalogo workspace --somente-local
"""
import argparse
import io
import os
from pathlib import Path

import psycopg2


def carregar_env(caminho: Path = Path(".env")) -> None:
    if caminho.exists():
        for linha in caminho.read_text(encoding="utf-8").splitlines():
            if linha.strip() and not linha.startswith("#") and "=" in linha:
                chave, valor = linha.split("=", 1)
                os.environ.setdefault(chave.strip(), valor.strip().strip('"'))


def extrair(pg_uri: str) -> dict[str, bytes]:
    """Retorna {tabela: conteúdo CSV} de todas as tabelas do schema public."""
    arquivos = {}
    with psycopg2.connect(pg_uri, connect_timeout=20) as conexao, conexao.cursor() as cur:
        conexao.set_client_encoding("UTF8")
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        for (nome,) in cur.fetchall():
            buffer = io.StringIO()
            cur.copy_expert(f'COPY (SELECT * FROM public."{nome}") TO STDOUT WITH (FORMAT csv, HEADER true)', buffer)
            arquivos[nome] = buffer.getvalue().encode("utf-8")
            print(f"extraído  {nome:<10} {buffer.getvalue().count(chr(10)) - 1:>5} linhas")
    return arquivos


def enviar(arquivos: dict[str, bytes], catalogo: str) -> None:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()  # usa DATABRICKS_HOST e DATABRICKS_TOKEN
    base = f"/Volumes/{catalogo}/landing/dados"
    for nome, conteudo in arquivos.items():
        w.files.create_directory(f"{base}/{nome}")
        w.files.upload(f"{base}/{nome}/{nome}.csv", io.BytesIO(conteudo), overwrite=True)
        print(f"enviado   {base}/{nome}/{nome}.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalogo", default="workspace")
    parser.add_argument("--somente-local", action="store_true", help="grava os CSVs em ./landing em vez de enviar")
    args = parser.parse_args()

    carregar_env()
    arquivos = extrair(os.environ["PG_URI"])

    if args.somente_local:
        for nome, conteudo in arquivos.items():
            destino = Path("landing") / nome / f"{nome}.csv"
            destino.parent.mkdir(parents=True, exist_ok=True)
            destino.write_bytes(conteudo)
            print(f"gravado   {destino}")
    else:
        enviar(arquivos, args.catalogo)


if __name__ == "__main__":
    main()
