# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata (limpeza, tipos, qualidade e MERGE)
# MAGIC
# MAGIC | Tabela | Chave | Uma linha por |
# MAGIC | --- | --- | --- |
# MAGIC | `prata_clima` | `hora` | hora (via `arrays_zip` + `explode`) |
# MAGIC | `prata_relatos` | `id` | relato (versão mais recente) |
# MAGIC | `prata_ciclovias` / `prata_ciclovia_segmentos` | `ciclovia_id` | trecho do GeoSampa / segmento de reta |
# MAGIC | `prata_qualidade` | | regra de qualidade, com o total barrado |
# MAGIC | `prata_clima_rejeitados`, `prata_relatos_rejeitados` | | linha barrada, com o motivo |
# MAGIC
# MAGIC **Regras de qualidade (item 06):**
# MAGIC 1. coordenada fora da cidade de São Paulo (lat −24,01 a −23,35; lon −46,83 a −46,36) ou nula;
# MAGIC 2. chuva nula ou negativa (`precipitacao_mm IS NULL OR < 0`; zero é chuva válida);
# MAGIC 3. severidade fora da lista `baixa`, `media`, `alta` (depois de `trim` + `lower`).
# MAGIC
# MAGIC Linhas barradas não entram na Prata e ficam nas tabelas `_rejeitados` para auditoria.

# COMMAND ----------

import json
import math
import re

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace")
CAT = dbutils.widgets.get("catalogo")
T = lambda nome: f"{CAT}.gs.{nome}"  # noqa: E731

SP_LAT_MIN, SP_LAT_MAX = -24.01, -23.35
SP_LON_MIN, SP_LON_MAX = -46.83, -46.36
SEVERIDADES = ["baixa", "media", "alta"]


def gravar(df, tabela):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(tabela))


def criar_se_nao_existe(df, tabela):
    if not spark.catalog.tableExists(T(tabela)):
        df.limit(0).write.saveAsTable(T(tabela))

# COMMAND ----------

# MAGIC %md ## Clima: uma linha por hora

# COMMAND ----------

clima = (
    spark.table(T("bronze_clima"))
    .select(
        "_arquivo",
        "_carregado_em",
        F.explode(
            F.arrays_zip(
                "hourly.time",
                "hourly.precipitation",
                "hourly.rain",
                "hourly.temperature_2m",
                "hourly.relative_humidity_2m",
            )
        ).alias("h"),
    )
    # dá nome aos campos do struct pela posição (independe da versão do Spark)
    .withColumn(
        "h",
        F.col("h").cast(
            "struct<hora:string, precipitacao_mm:double, chuva_mm:double, temperatura_c:double, umidade_pct:double>"
        ),
    )
    .select(
        F.to_timestamp("h.hora", "yyyy-MM-dd'T'HH:mm").alias("hora"),  # já em horário de SP
        "h.precipitacao_mm",
        "h.chuva_mm",
        "h.temperatura_c",
        F.col("h.umidade_pct").cast("int").alias("umidade_pct"),
        "_arquivo",
        "_carregado_em",
    )
    .withColumn("falha_chuva", F.col("precipitacao_mm").isNull() | (F.col("precipitacao_mm") < 0))
)

# COMMAND ----------

# MAGIC %md ## Relatos: tipos, normalização e regras

# COMMAND ----------

bronze_relatos = spark.table(T("bronze_relatos"))
relatos = (
    bronze_relatos.select(
        F.trim("id").alias("id"),
        # o app manda horário local com fuso (-03:00); guardamos a hora local
        F.to_timestamp(F.substring("criado_em", 1, 19)).alias("criado_em"),
        F.to_timestamp(F.substring("atualizado_em", 1, 19)).alias("atualizado_em"),
        F.col("lat").cast("double").alias("lat"),
        F.col("lon").cast("double").alias("lon"),
        F.lower(F.trim("tipo_perigo")).alias("tipo_perigo"),
        F.col("severidade").alias("severidade_original"),
        F.lower(F.trim("severidade")).alias("severidade"),
        "descricao",
        F.lower(F.trim("status")).alias("status"),
        "usuario_id",
        "origem",
        "_arquivo",
        "_carregado_em",
    )
    .withColumn(
        "falha_coordenada",
        F.col("lat").isNull()
        | F.col("lon").isNull()
        | ~F.col("lat").between(SP_LAT_MIN, SP_LAT_MAX)
        | ~F.col("lon").between(SP_LON_MIN, SP_LON_MAX),
    )
    .withColumn("falha_severidade", F.col("severidade").isNull() | ~F.col("severidade").isin(SEVERIDADES))
)

# COMMAND ----------

# MAGIC %md ## Total barrado por regra (tabela do item 06)

# COMMAND ----------

n_clima = clima.count()
n_relatos = relatos.count()
qualidade = spark.createDataFrame(
    [
        ("1. coordenada fora de SP", "relatos", n_relatos, relatos.filter("falha_coordenada").count()),
        ("2. chuva nula ou negativa", "clima", n_clima, clima.filter("falha_chuva").count()),
        ("3. severidade fora da lista", "relatos", n_relatos, relatos.filter("falha_severidade").count()),
    ],
    "regra STRING, fonte STRING, linhas_avaliadas BIGINT, linhas_barradas BIGINT",
).withColumn("verificado_em", F.current_timestamp())
gravar(qualidade, "prata_qualidade")

# Informativo (não são regras de barragem):
corrigidas = relatos.filter(
    ~F.col("falha_severidade") & (F.col("severidade_original") != F.col("severidade"))
).count()
validos = relatos.filter("NOT falha_coordenada AND NOT falha_severidade")
print(f"Severidades só mal formatadas e corrigidas com trim/lower: {corrigidas}")
print(f"Linhas repetidas por id (duplicatas + versões antigas), resolvidas antes do MERGE: {validos.count() - validos.select('id').distinct().count()}")
display(spark.table(T("prata_qualidade")))

# COMMAND ----------

motivo_relato = F.concat_ws(
    "; ",
    F.when(F.col("falha_coordenada"), F.lit("coordenada fora de SP")),
    F.when(F.col("falha_severidade"), F.lit("severidade fora da lista")),
)
gravar(
    relatos.filter("falha_coordenada OR falha_severidade").withColumn("motivo", motivo_relato).drop("falha_coordenada", "falha_severidade"),
    "prata_relatos_rejeitados",
)
gravar(clima.filter("falha_chuva").withColumn("motivo", F.lit("chuva nula ou negativa")).drop("falha_chuva"), "prata_clima_rejeitados")

# COMMAND ----------

# MAGIC %md ## MERGE pela chave
# MAGIC Antes do MERGE, cada chave fica com uma linha só (a mais recente). Se duas linhas
# MAGIC da origem casarem com a mesma linha do destino, o MERGE falha.

# COMMAND ----------

ultimo_clima = Window.partitionBy("hora").orderBy(F.desc("_carregado_em"), F.desc("_arquivo"))
(
    clima.filter("NOT falha_chuva AND hora IS NOT NULL")
    .withColumn("_n", F.row_number().over(ultimo_clima))
    .filter("_n = 1")
    .drop("_n", "falha_chuva")
    .createOrReplaceTempView("clima_lote")
)

ultimo_relato = Window.partitionBy("id").orderBy(F.desc("atualizado_em"), F.desc("_carregado_em"), F.desc("_arquivo"))
(
    relatos.filter("NOT falha_coordenada AND NOT falha_severidade AND id IS NOT NULL")
    .withColumn("_n", F.row_number().over(ultimo_relato))
    .filter("_n = 1")
    .drop("_n", "falha_coordenada", "falha_severidade", "severidade_original")
    .createOrReplaceTempView("relatos_lote")
)

criar_se_nao_existe(spark.table("clima_lote"), "prata_clima")
criar_se_nao_existe(spark.table("relatos_lote"), "prata_relatos")

# COMMAND ----------

resultado_clima = spark.sql(f"""
  MERGE INTO {T('prata_clima')} AS t
  USING clima_lote AS s
  ON t.hora = s.hora
  WHEN MATCHED AND s._carregado_em > t._carregado_em THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
""")
display(resultado_clima)

# COMMAND ----------

resultado_relatos = spark.sql(f"""
  MERGE INTO {T('prata_relatos')} AS t
  USING relatos_lote AS s
  ON t.id = s.id
  WHEN MATCHED AND s.atualizado_em > t.atualizado_em THEN UPDATE SET *   -- ex.: status aberto -> resolvido
  WHEN NOT MATCHED THEN INSERT *
""")
display(resultado_relatos)

# COMMAND ----------

# MAGIC %md ## Ciclovias (GeoSampa)
# MAGIC Cada trecho vira uma linha em `prata_ciclovias` e seus segmentos de reta vão para
# MAGIC `prata_ciclovia_segmentos`, já em metros (projeção plana local, erro < 0,5% na
# MAGIC escala da cidade). A Ouro usa os segmentos para achar a ciclovia de cada relato.
# MAGIC
# MAGIC Os nomes dos campos do GeoSampa podem mudar. O código escolhe sozinho o campo de
# MAGIC nome e o de tipo. Se escolher errado, preencha `CAMPO_NOME` / `CAMPO_TIPO`.

# COMMAND ----------

CAMPO_NOME = None  # GeoSampa (nov/2026): "nm_trecho_via_bicicleta"
CAMPO_TIPO = None  # GeoSampa (nov/2026): "tx_tipo_via_bicicleta"

LAT0 = -23.55
KX = 111320.0 * math.cos(math.radians(LAT0))  # metros por grau de longitude
KY = 110574.0  # metros por grau de latitude

# Usado só se a carga do GeoSampa falhou (traçados aproximados, documentado).
CICLOVIAS_RESERVA = {
    "Av. Paulista": [[-46.6610, -23.5570], [-46.6530, -23.5630], [-46.6430, -23.5716]],
    "Av. Brig. Faria Lima": [[-46.6930, -23.5670], [-46.6870, -23.5770], [-46.6810, -23.5870]],
    "Ciclovia Rio Pinheiros": [[-46.7430, -23.5340], [-46.7280, -23.5700], [-46.7160, -23.6200], [-46.7050, -23.6720]],
    "Radial Leste": [[-46.6150, -23.5450], [-46.5700, -23.5420], [-46.5200, -23.5400]],
    "Av. Eliseu de Almeida": [[-46.7300, -23.5750], [-46.7450, -23.5810], [-46.7600, -23.5880]],
    "Av. República do Líbano": [[-46.6600, -23.5850], [-46.6615, -23.5950], [-46.6630, -23.6050]],
    "R. da Consolação": [[-46.6440, -23.5480], [-46.6530, -23.5520], [-46.6610, -23.5560]],
    "Av. Sumaré": [[-46.6820, -23.5460], [-46.6795, -23.5380], [-46.6770, -23.5300]],
}


def escolher_campo(chaves, padroes):
    for padrao in padroes:
        for chave in chaves:
            if re.search(padrao, chave.lower()):
                return chave
    return None


def haversine_m(a, b):
    p1, p2 = math.radians(a[1]), math.radians(b[1])
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(b[0] - a[0]) / 2) ** 2
    return 2 * 6371000 * math.asin(math.sqrt(h))


bruto = spark.table(T("bronze_ciclovias")).orderBy(F.desc("_carregado_em"), F.desc("_arquivo")).limit(1).collect()
if bruto:
    feats = json.loads(bruto[0]["geojson"]).get("features", [])
    origem_ciclovias = "GeoSampa"
else:
    print("ATENÇÃO: bronze_ciclovias vazia. Usando traçados de reserva (rode gs_carga_ciclovias).")
    feats = [
        {"id": f"reserva.{i}", "properties": {"nome": nome}, "geometry": {"type": "LineString", "coordinates": coords}}
        for i, (nome, coords) in enumerate(CICLOVIAS_RESERVA.items(), 1)
    ]
    origem_ciclovias = "reserva"

chaves = sorted({k for f in feats for k in (f.get("properties") or {})})
campo_nome = CAMPO_NOME or escolher_campo(
    chaves, [r"nm_trecho", r"logradouro", r"nome", r"denomina", r"^nm_(?!orgao|programa|regiao)"]
)
campo_tipo = CAMPO_TIPO or escolher_campo(chaves, [r"^tp_", r"tipo", r"classe", r"categoria"])
print(f"Campos disponíveis: {chaves}")
print(f"Campo de nome: {campo_nome} | campo de tipo: {campo_tipo}")

linhas_ciclovia, segmentos = [], []
for i, feat in enumerate(feats, 1):
    geom = feat.get("geometry") or {}
    partes = [geom.get("coordinates", [])] if geom.get("type") == "LineString" else geom.get("coordinates", []) if geom.get("type") == "MultiLineString" else []
    partes = [p for p in partes if len(p) >= 2]
    if not partes:
        continue
    props = feat.get("properties") or {}
    cid = str(feat.get("id") or f"trecho.{i}")
    nome = str(props.get(campo_nome) or "").strip() if campo_nome else ""
    nome = re.sub(r"\s*-\s*TRECHO\s*\d+\s*$", "", nome, flags=re.I)  # junta "X - TRECHO 1/2" em "X"
    vertices = [v for p in partes for v in p]
    comprimento = 0.0
    for parte in partes:
        for a, b in zip(parte, parte[1:]):
            comprimento += haversine_m(a, b)
            segmentos.append((cid, a[0] * KX, a[1] * KY, b[0] * KX, b[1] * KY))
    linhas_ciclovia.append(
        (
            cid,
            nome or f"Trecho sem nome ({cid})",
            str(props.get(campo_tipo)) if campo_tipo and props.get(campo_tipo) is not None else None,
            round(comprimento, 1),
            sum(v[1] for v in vertices) / len(vertices),
            sum(v[0] for v in vertices) / len(vertices),
            json.dumps(props, ensure_ascii=False, default=str),
            origem_ciclovias,
        )
    )

gravar(
    spark.createDataFrame(
        linhas_ciclovia,
        "ciclovia_id STRING, nome STRING, tipo STRING, comprimento_m DOUBLE, centro_lat DOUBLE, centro_lon DOUBLE, propriedades STRING, origem STRING",
    ),
    "prata_ciclovias",
)
gravar(
    spark.createDataFrame(segmentos, "ciclovia_id STRING, x1 DOUBLE, y1 DOUBLE, x2 DOUBLE, y2 DOUBLE"),
    "prata_ciclovia_segmentos",
)
print(f"{len(linhas_ciclovia)} trechos, {len(segmentos)} segmentos, {sum(l[3] for l in linhas_ciclovia) / 1000:.1f} km")

# COMMAND ----------

for tabela in ["prata_clima", "prata_relatos", "prata_ciclovias", "prata_ciclovia_segmentos"]:
    print(f"{tabela}: {spark.table(T(tabela)).count()} linhas")