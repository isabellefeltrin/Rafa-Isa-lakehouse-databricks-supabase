"""Gera sql/01_seguradora_postgres.sql com dados de exemplo (deterministico)."""
import random
import sys
from datetime import date, timedelta

random.seed(42)
OUT = sys.argv[1]

def cpf_valido(base9):
    d = [int(c) for c in base9]
    s = sum(v * w for v, w in zip(d, range(10, 1, -1)))
    d1 = 0 if s % 11 < 2 else 11 - s % 11
    d.append(d1)
    s = sum(v * w for v, w in zip(d, range(11, 1, -1)))
    d2 = 0 if s % 11 < 2 else 11 - s % 11
    d.append(d2)
    return "".join(map(str, d))

def q(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, date):
        return f"'{v.isoformat()}'"
    return "'" + str(v).replace("'", "''") + "'"

def row(vals):
    return "(" + ", ".join(q(v) for v in vals) + ")"

marcas = [(1, "RENAULT"), (2, "HYUNDAI"), (3, "FIAT"), (4, "FORD"), (5, "GM"),
          (6, "VOLKSWAGEN"), (7, "TOYOTA"), (8, "HONDA"), (9, " jeep ")]  # 9: sujo (espacos/minusculo)

modelos = {1: ["CLIO", "KWID", "SANDERO", "DUSTER"], 2: ["CRETA", "HB20"], 3: ["PUNTO", "PALIO", "ARGO", "MOBI", "TORO"],
           4: ["ECO SPORT", "KA", "RANGER"], 5: ["S10", "ONIX", "TRACKER"], 6: ["TIGUAN", "PASSAT", "GOL", "POLO", "T-CROSS"],
           7: ["COROLLA", "HILUX", "YARIS"], 8: ["CIVIC", "HR-V", "FIT"], 9: ["RENEGADE", "COMPASS"]}
cores = ["BRANCO", "PRETO", "PRATA", "CINZA", "AZUL", "VERMELHO", "AMARELO"]
cidades = [("Criciúma", "SC"), ("Florianópolis", "SC"), ("Tubarão", "SC"), ("Içara", "SC"), ("Araranguá", "SC"),
           ("Joinville", "SC"), ("Blumenau", "SC"), ("Porto Alegre", "RS"), ("Curitiba", "PR"), ("São Paulo", "SP")]
primeiros = ["Ana", "Bruno", "Carla", "Diego", "Eduarda", "Felipe", "Gabriela", "Henrique", "Isadora", "João",
             "Larissa", "Marcos", "Natália", "Otávio", "Patrícia", "Rafael", "Sabrina", "Tiago", "Vanessa", "Wagner"]
sobrenomes = ["Souza", "Lima", "Pereira", "Martins", "Silva", "Costa", "Rocha", "Alves", "Ribeiro", "Fernandes",
              "Gomes", "Machado", "Zanette", "Bortolotto", "De Luca"]

# ---------------- clientes ----------------
clientes = [
    (1, "Ana Souza", cpf_valido("111444777"), "ana.souza@email.com", "Criciúma", "SC", date(1985, 3, 12)),
    (2, "Bruno Lima", cpf_valido("222555888"), "bruno.lima@email.com", "Florianópolis", "SC", date(1990, 7, 1)),
    (3, "Carla Pereira", cpf_valido("333666999"), "carla.p@email.com", "Tubarão", "SC", date(1978, 11, 23)),
    (4, "Diego Martins", cpf_valido("444777000"), "diego.m@email.com", "Içara", "SC", date(1995, 1, 30)),
]
usados = set()
for i in range(5, 41):
    nome = f"{random.choice(primeiros)} {random.choice(sobrenomes)}"
    while nome in usados:
        nome = f"{random.choice(primeiros)} {random.choice(sobrenomes)}"
    usados.add(nome)
    cid, uf = random.choice(cidades)
    base = "".join(str(random.randint(0, 9)) for _ in range(9))
    email = nome.lower().replace(" ", ".").translate(str.maketrans("áãâéêíóôõúç", "aaaeeiooouc")) + "@email.com"
    nasc = date(1950, 1, 1) + timedelta(days=random.randint(0, 365 * 55))
    clientes.append([i, nome, cpf_valido(base), email, cid, uf, nasc])

# registros sujos (corrigiveis e rejeitaveis)
c = clientes
c[5] = list(c[5]); cpf = c[5][2]; c[5][2] = f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"   # CPF com mascara -> corrige
c[6] = list(c[6]); c[6][1] = "  " + c[6][1].upper() + "  "                                   # nome sujo -> corrige
c[7] = list(c[7]); c[7][4] = " criciúma "; c[7][5] = "sc"                                     # cidade/UF -> corrige
c[8] = list(c[8]); c[8][3] = "email-invalido"                                                 # e-mail -> vira NULL
c[9] = list(c[9]); c[9][2] = c[9][2][:-1] + str((int(c[9][2][-1]) + 1) % 10)                   # DV errado -> rejeita
c[10] = list(c[10]); c[10][2] = "1234567890"                                                  # 10 digitos -> rejeita
c[11] = list(c[11]); c[11][2] = "00000000000"                                                 # CPF invalido -> rejeita
c[12] = list(c[12]); c[12][6] = date(2031, 5, 5)                                              # nascimento futuro -> rejeita
c[13] = list(c[13]); c[13][4] = None                                                          # cidade nula -> 'NAO INFORMADA'

# ---------------- carros ----------------
carros = [
    ("ALD3834", "CLIO", "34574215969", 1, 2011, "BRANCO", 1),
    ("CCR8096", "CRETA", "88547875547", 2, 2020, "BRANCO", 1),
    ("DLA3438", "PUNTO", "98823483434", 3, 2013, "PRETO", 2),
    ("EEE1056", "ECO SPORT", "56753453455", 4, 2020, "AZUL", 2),
    ("FFR1234", "PALIO", "32383478747", 3, 2020, "AMARELO", 3),
    ("GQY6753", "S10", "72004160549", 5, 2015, "PRETO", 3),
    ("IAC8974", "TIGUAN", "77130757746", 6, 2020, "AZUL", 4),
    ("JIE0952", "PASSAT", "87493270405", 6, 2016, "CINZA", 4),
]
placas = {p[0] for p in carros}
letras = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
while len(carros) < 60:
    mercosul = random.random() < 0.6
    p = "".join(random.choice(letras) for _ in range(3)) + str(random.randint(0, 9)) + \
        (random.choice(letras) if mercosul else str(random.randint(0, 9))) + f"{random.randint(0, 99):02d}"
    if p in placas:
        continue
    placas.add(p)
    m = random.randint(1, 9)
    carros.append([p, random.choice(modelos[m]), "".join(str(random.randint(0, 9)) for _ in range(11)), m,
                   random.randint(2008, 2025), random.choice(cores), random.randint(1, 40)])

carros[10] = list(carros[10]); carros[10][0] = carros[10][0].lower()       # placa minuscula -> corrige
carros[11] = list(carros[11]); carros[11][1] = " " + carros[11][1].lower()  # modelo sujo -> corrige
carros[12] = list(carros[12]); carros[12][4] = 2035                         # ano futuro -> rejeita
carros[13] = list(carros[13]); carros[13][5] = None                         # cor nula -> 'NAO INFORMADA'
carros[14] = list(carros[14]); carros[14][0] = "AB12345"                    # placa fora do padrao -> rejeita

# ---------------- apolices ----------------
apolices = [
    (1, "ALD3834", 1850.00, date(2024, 1, 10)), (2, "CCR8096", 2790.50, date(2024, 2, 15)),
    (3, "DLA3438", 1620.00, date(2024, 3, 1)), (4, "EEE1056", 2310.75, date(2024, 3, 20)),
    (5, "FFR1234", 1980.00, date(2024, 4, 5)), (6, "GQY6753", 3150.00, date(2024, 5, 12)),
    (7, "IAC8974", 3890.90, date(2024, 6, 1)), (8, "JIE0952", 3420.00, date(2024, 6, 18)),
]
apolices = [[a[0], a[1], a[2], a[3], a[3] + timedelta(days=365)] for a in apolices]
aid = 9
for car in carros[8:]:
    ini = date(2024, 1, 1) + timedelta(days=random.randint(0, 500))
    apolices.append([aid, car[0], round(random.uniform(1200, 5200), 2), ini, ini + timedelta(days=365)]); aid += 1
for a in list(apolices[:16]):  # renovacoes
    ini = a[4]
    apolices.append([aid, a[1], round(a[2] * random.uniform(1.03, 1.12), 2), ini, ini + timedelta(days=365)]); aid += 1

apolices[20][2] = -950.00                                              # premio negativo -> rejeita
apolices[21][4] = apolices[21][3] - timedelta(days=30)                 # fim antes do inicio -> rejeita
apolices[22][3] = date(2027, 3, 1); apolices[22][4] = date(2028, 3, 1)  # inicio futuro -> rejeita

# ---------------- sinistros ----------------
tipos = ["COLISAO", "ROUBO", "FURTO", "VIDROS", "INCENDIO", "ALAGAMENTO", "TERCEIROS"]
faixa = {"COLISAO": (800, 6000), "ROUBO": (25000, 60000), "FURTO": (20000, 50000), "VIDROS": (200, 1500),
         "INCENDIO": (15000, 45000), "ALAGAMENTO": (3000, 12000), "TERCEIROS": (500, 5000)}
pesos = [36, 2, 2, 32, 1, 4, 23]
sinistros = []
sid = 1
validas = [a for a in apolices if a[2] > 0 and a[4] > a[3] and a[3] < date(2026, 1, 1)]
while len(sinistros) < 45:
    a = random.choice(validas)
    fim = min(a[4], date(2026, 9, 30))
    if fim <= a[3]:
        continue
    d = a[3] + timedelta(days=random.randint(0, (fim - a[3]).days))
    t = random.choices(tipos, pesos)[0]
    sinistros.append([sid, a[0], d, t, round(random.uniform(*faixa[t]), 2)]); sid += 1

s = sinistros
acento = {"COLISAO": "Colisão", "INCENDIO": "Incêndio"}
s[3][3] = f" {s[3][3].lower()} "                                        # tipo despadronizado -> corrige
s[4][3] = acento.get(s[4][3], s[4][3].title())
s[5][3] = s[5][3].lower()
s[6][4] = -1200.00                                                      # valor negativo -> rejeita
s[7][2] = date(2027, 1, 15)                                             # data futura -> rejeita
s[8][2] = [a for a in apolices if a[0] == s[8][1]][0][3] - timedelta(days=40)  # fora da vigencia -> rejeita
for k in (10, 11, 12):                                                  # duplicados (mesmo evento, outro id)
    sinistros.append([sid, s[k][1], s[k][2], s[k][3], s[k][4]]); sid += 1
s[13][4] = None                                                         # valor nulo -> rejeita

# ---------------- escrita ----------------
L = []
L.append("""-- =====================================================================
-- Banco de origem do Trabalho 3: seguradora (PostgreSQL no Supabase)
-- Gerado por scripts/gerar_dados.py. Contem registros "sujos" DE PROPOSITO
-- para exercitar as regras de Data Quality da camada SILVER.
-- Execute inteiro no Supabase: SQL Editor > New query > colar > Run.
-- =====================================================================

DROP TABLE IF EXISTS sinistro;
DROP TABLE IF EXISTS apolice;
DROP TABLE IF EXISTS carro;
DROP TABLE IF EXISTS cliente;
DROP TABLE IF EXISTS marca;

CREATE TABLE marca (
    id   INT PRIMARY KEY,
    nome VARCHAR(40) NOT NULL
);

CREATE TABLE cliente (
    id              INT PRIMARY KEY,
    nome            VARCHAR(80) NOT NULL,
    cpf             VARCHAR(14) NOT NULL,
    email           VARCHAR(120),
    cidade          VARCHAR(60),
    uf              VARCHAR(2),
    data_nascimento DATE
);

CREATE TABLE carro (
    placa      VARCHAR(8) PRIMARY KEY,
    modelo     VARCHAR(40) NOT NULL,
    chassi     VARCHAR(20) NOT NULL,
    marca_id   INT REFERENCES marca (id),
    ano        INT,
    cor        VARCHAR(20),
    cliente_id INT REFERENCES cliente (id)
);

CREATE TABLE apolice (
    id          INT PRIMARY KEY,
    placa       VARCHAR(8) REFERENCES carro (placa),
    valor       NUMERIC(10, 2) NOT NULL,
    data_inicio DATE NOT NULL,
    data_fim    DATE NOT NULL
);

CREATE TABLE sinistro (
    id              INT PRIMARY KEY,
    apolice_id      INT REFERENCES apolice (id),
    data_ocorrencia DATE NOT NULL,
    tipo            VARCHAR(30) NOT NULL,
    valor           NUMERIC(12, 2)
);
""")

def bloco(tabela, linhas):
    L.append(f"INSERT INTO {tabela} VALUES\n    " + ",\n    ".join(row(r) for r in linhas) + ";\n")

bloco("marca", marcas)
bloco("cliente", clientes)
bloco("carro", carros)
bloco("apolice", apolices)
bloco("sinistro", sinistros)
L.append("""-- Conferencia rapida
SELECT 'marca' AS tabela, COUNT(*) FROM marca
UNION ALL SELECT 'cliente', COUNT(*) FROM cliente
UNION ALL SELECT 'carro', COUNT(*) FROM carro
UNION ALL SELECT 'apolice', COUNT(*) FROM apolice
UNION ALL SELECT 'sinistro', COUNT(*) FROM sinistro;
""")
open(OUT, "w", encoding="utf-8").write("\n".join(L))
print(len(marcas), len(clientes), len(carros), len(apolices), len(sinistros))
