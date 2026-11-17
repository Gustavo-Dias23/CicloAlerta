# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ouro (modelo dimensional + indicadores do MVP)
# MAGIC
# MAGIC ```
# MAGIC                 dim_tempo (sk_tempo)
# MAGIC                        │
# MAGIC dim_ciclovia ──── fato_relato ──── dim_tipo_perigo
# MAGIC (ciclovia_id)   1 linha por relato   (tipo_perigo_id)
# MAGIC                 + chuva daquela hora
# MAGIC ```
# MAGIC
# MAGIC - **Relato → ciclovia:** menor distância do ponto até um segmento de ciclovia; até
# MAGIC   `RAIO_M` metros conta como "na ciclovia", acima disso vira `-1 · Fora da rede cicloviária`.
# MAGIC - **Hora de chuva:** precipitação daquela hora ≥ `CHUVA_MIN_MM` (0,1 mm, a menor medida do Open-Meteo).
# MAGIC - **Alerta ao ciclista:** severidade `alta` + hora de chuva + dentro da rede.
# MAGIC - **LGPD:** `usuario_id` não entra na Ouro. No lugar dele vai
# MAGIC   `usuario_hash = sha2(usuario_id || salt, 256)`. O salt fica fora do repositório:
# MAGIC   num secret (`gs/salt_lgpd`) ou digitado no widget na hora de rodar. Usem sempre o mesmo valor.
# MAGIC - Relatos com status `descartado` ficam na fato, mas saem dos indicadores.

# COMMAND ----------

import math

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("salt_lgpd", "")
CAT = dbutils.widgets.get("catalogo")
T = lambda nome: f"{CAT}.gs.{nome}"  # noqa: E731

RAIO_M = 30.0
CHUVA_MIN_MM = 0.1

try:
    SALT = dbutils.secrets.get("gs", "salt_lgpd")
except Exception:  # noqa: BLE001 - sem secret scope no Free Edition
    SALT = dbutils.widgets.get("salt_lgpd")
if not SALT:
    raise ValueError("Preencha o widget salt_lgpd (qualquer texto secreto; não salve no repositório).")


def gravar(df, tabela):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(tabela))

# COMMAND ----------

# MAGIC %md ## dim_tipo_perigo

# COMMAND ----------

dim_tipo_perigo = spark.createDataFrame(
    [
        (0, "desconhecido", "Tipo não informado", "outros"),
        (1, "buraco", "Buraco ou asfalto afundado", "infraestrutura"),
        (2, "obstrucao", "Obstrução na ciclovia", "uso indevido"),
        (3, "conflito_veiculo", "Conflito com veículo", "trânsito"),
        (4, "iluminacao_ruim", "Iluminação ruim", "infraestrutura"),
        (5, "alagamento", "Alagamento", "clima"),
        (6, "piso_escorregadio", "Piso escorregadio", "clima"),
        (7, "sinalizacao_apagada", "Sinalização apagada", "infraestrutura"),
    ],
    "tipo_perigo_id INT, tipo_perigo STRING, descricao STRING, categoria STRING",
)
gravar(dim_tipo_perigo, "dim_tipo_perigo")

# COMMAND ----------

# MAGIC %md ## dim_ciclovia

# COMMAND ----------

dim_ciclovia = spark.table(T("prata_ciclovias")).select(
    "ciclovia_id", "nome", "tipo", F.round(F.col("comprimento_m") / 1000, 3).alias("comprimento_km"), "centro_lat", "centro_lon"
)
fora_da_rede = spark.createDataFrame(
    [("-1", "Fora da rede cicloviária", None, None, None, None)],
    dim_ciclovia.schema,
)
gravar(dim_ciclovia.unionByName(fora_da_rede), "dim_ciclovia")

# COMMAND ----------

# MAGIC %md ## Relato → ciclovia mais próxima

# COMMAND ----------

LAT0 = -23.55
KX = 111320.0 * math.cos(math.radians(LAT0))
KY = 110574.0

relatos = spark.table(T("prata_relatos")).withColumn("px", F.col("lon") * KX).withColumn("py", F.col("lat") * KY)
segmentos = spark.table(T("prata_ciclovia_segmentos"))

# Filtro grosso pela caixa do segmento (+ raio), depois distância exata ponto-segmento.
candidatos = relatos.select("id", "px", "py").join(
    segmentos,
    (F.col("px") >= F.least("x1", "x2") - RAIO_M)
    & (F.col("px") <= F.greatest("x1", "x2") + RAIO_M)
    & (F.col("py") >= F.least("y1", "y2") - RAIO_M)
    & (F.col("py") <= F.greatest("y1", "y2") + RAIO_M),
)
dx, dy = F.col("x2") - F.col("x1"), F.col("y2") - F.col("y1")
t = ((F.col("px") - F.col("x1")) * dx + (F.col("py") - F.col("y1")) * dy) / (dx * dx + dy * dy)
t = F.when((dx * dx + dy * dy) == 0, F.lit(0.0)).otherwise(F.greatest(F.lit(0.0), F.least(F.lit(1.0), t)))
distancia = F.sqrt(
    F.pow(F.col("px") - (F.col("x1") + t * dx), 2) + F.pow(F.col("py") - (F.col("y1") + t * dy), 2)
)
mais_proxima = (
    candidatos.withColumn("distancia_m", distancia)
    .filter(F.col("distancia_m") <= RAIO_M)
    .withColumn("_n", F.row_number().over(Window.partitionBy("id").orderBy("distancia_m", "ciclovia_id")))
    .filter("_n = 1")
    .select("id", "ciclovia_id", F.round("distancia_m", 1).alias("distancia_ciclovia_m"))
)

# COMMAND ----------

# MAGIC %md ## fato_relato

# COMMAND ----------

clima_hora = spark.table(T("prata_clima")).select(F.col("hora").alias("hora_cheia"), "precipitacao_mm")
sk = lambda col: F.date_format(col, "yyyyMMddHH").cast("bigint")  # noqa: E731

fato = (
    relatos.withColumn("hora_cheia", F.date_trunc("hour", "criado_em"))
    .join(mais_proxima, "id", "left")
    .join(clima_hora, "hora_cheia", "left")
    .join(dim_tipo_perigo.select("tipo_perigo", "tipo_perigo_id"), "tipo_perigo", "left")
    .select(
        F.col("id").alias("relato_id"),
        sk("hora_cheia").alias("sk_tempo"),
        F.coalesce("ciclovia_id", F.lit("-1")).alias("ciclovia_id"),
        F.coalesce("tipo_perigo_id", F.lit(0)).alias("tipo_perigo_id"),
        "severidade",
        "status",
        "lat",
        "lon",
        "distancia_ciclovia_m",
        "precipitacao_mm",
        (F.coalesce("precipitacao_mm", F.lit(0.0)) >= CHUVA_MIN_MM).alias("em_chuva"),
        "criado_em",
        "atualizado_em",
        F.sha2(F.concat_ws("|", "usuario_id", F.lit(SALT)), 256).alias("usuario_hash"),  # LGPD
    )
    .withColumn(
        "alerta_ciclista",
        (F.col("severidade") == "alta") & F.col("em_chuva") & (F.col("ciclovia_id") != "-1"),
    )
)
gravar(fato, "fato_relato")

# COMMAND ----------

# MAGIC %md ## dim_tempo

# COMMAND ----------

horas = (
    spark.table(T("prata_clima")).select(F.col("hora").alias("hora_cheia"), "precipitacao_mm")
    .unionByName(
        spark.table(T("fato_relato")).select(
            F.to_timestamp(F.col("sk_tempo").cast("string"), "yyyyMMddHH").alias("hora_cheia"),
            F.lit(None).cast("double").alias("precipitacao_mm"),
        )
    )
    .groupBy("hora_cheia")
    .agg(F.max("precipitacao_mm").alias("precipitacao_mm"))
)
nomes_dia = F.element_at(
    F.array(*[F.lit(d) for d in ["domingo", "segunda", "terça", "quarta", "quinta", "sexta", "sábado"]]),
    F.dayofweek("hora_cheia"),
)
dim_tempo = horas.select(
    sk("hora_cheia").alias("sk_tempo"),
    "hora_cheia",
    F.to_date("hora_cheia").alias("data"),
    F.hour("hora_cheia").alias("hora"),
    F.dayofweek("hora_cheia").alias("dia_semana"),
    nomes_dia.alias("nome_dia"),
    F.dayofweek("hora_cheia").isin(1, 7).alias("fim_de_semana"),
    F.when(F.hour("hora_cheia") < 6, "madrugada")
    .when(F.hour("hora_cheia") < 12, "manhã")
    .when(F.hour("hora_cheia") < 18, "tarde")
    .otherwise("noite")
    .alias("periodo"),
    "precipitacao_mm",
    (F.coalesce("precipitacao_mm", F.lit(0.0)) >= CHUVA_MIN_MM).alias("hora_com_chuva"),
)
gravar(dim_tempo, "dim_tempo")

# COMMAND ----------

# MAGIC %md ## ouro_indicadores (tabela que o MVP usa) + CSV

# COMMAND ----------

indicadores = (
    spark.table(T("fato_relato"))
    .filter(F.col("status") != "descartado")
    .join(spark.table(T("dim_ciclovia")), "ciclovia_id")
    .join(spark.table(T("dim_tipo_perigo")).select("tipo_perigo_id", F.col("descricao").alias("tipo_desc")), "tipo_perigo_id")
    .groupBy(F.col("nome").alias("ciclovia"))
    .agg(
        F.count("*").alias("total_perigos"),
        F.sum(F.when(F.col("severidade") == "alta", 1).otherwise(0)).alias("perigos_alta"),
        F.sum(F.col("em_chuva").cast("int")).alias("perigos_em_chuva"),
        F.sum(F.col("alerta_ciclista").cast("int")).alias("alertas_ciclista"),
        F.mode("tipo_desc").alias("tipo_mais_comum"),
        F.max("criado_em").alias("ultimo_relato"),
        F.round(F.avg("lat"), 6).alias("lat_media"),
        F.round(F.avg("lon"), 6).alias("lon_media"),
    )
    .withColumn("pct_em_chuva", F.round(100 * F.col("perigos_em_chuva") / F.col("total_perigos"), 1))
    .withColumn("atualizado_em", F.current_timestamp())
    .orderBy(F.desc("total_perigos"))
)
gravar(indicadores, "ouro_indicadores")

caminho_csv = f"/Volumes/{CAT}/gs/exportacao/ouro_indicadores.csv"
spark.table(T("ouro_indicadores")).orderBy(F.desc("total_perigos")).toPandas().to_csv(caminho_csv, index=False)
print(f"CSV gravado em {caminho_csv} (baixe pelo Catalog Explorer e coloque no repositório do app)")
display(spark.table(T("ouro_indicadores")))

# COMMAND ----------

# MAGIC %md ## Prova de LGPD (print do item 08)

# COMMAND ----------

display(spark.table(T("fato_relato")).select("relato_id", "usuario_hash", "severidade", "ciclovia_id", "em_chuva").limit(10))
print("Colunas da fato:", spark.table(T("fato_relato")).columns)
