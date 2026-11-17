# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze (Auto Loader, incremental)
# MAGIC
# MAGIC Lê os arquivos do Volume `chegada` **sem alterar o conteúdo**, acrescentando só
# MAGIC `_arquivo` (de onde veio) e `_carregado_em` (quando entrou).
# MAGIC
# MAGIC | Tabela | Origem | Uma linha por |
# MAGIC | --- | --- | --- |
# MAGIC | `bronze_clima` | `chegada/clima/clima_*.json` | arquivo (dia), com as 24 horas em arrays |
# MAGIC | `bronze_relatos` | `chegada/relatos/relatos_*.json` (JSON Lines) | relato |
# MAGIC | `bronze_ciclovias` | `chegada/ciclovias/*.geojson` (carga única) | arquivo, com o GeoJSON bruto |
# MAGIC
# MAGIC O Auto Loader guarda em `checkpoints/` quais arquivos já leu. Rodar de novo sem
# MAGIC arquivo novo **não muda a contagem**. É isso que o item 05 pede para provar.
# MAGIC
# MAGIC **Como usar o widget `momento`:** escolha o momento antes de rodar. A última célula
# MAGIC grava as contagens em `bronze_contagens`, que vira a tabela do relatório.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.dropdown(
    "momento",
    "1_antes_do_lote_2",
    ["1_antes_do_lote_2", "2_depois_do_lote_2", "3_reexecucao_sem_arquivo_novo"],
)
CAT = dbutils.widgets.get("catalogo")
MOMENTO = dbutils.widgets.get("momento")

CHEGADA = f"/Volumes/{CAT}/gs/chegada"
CHECKPOINTS = f"/Volumes/{CAT}/gs/checkpoints"
T = lambda nome: f"{CAT}.gs.{nome}"  # noqa: E731

# COMMAND ----------

# Esquemas explícitos: o Auto Loader não precisa adivinhar tipos, e um campo
# inesperado vai para _rescued_data em vez de quebrar a carga.
SCHEMA_CLIMA = """
  latitude DOUBLE, longitude DOUBLE, generationtime_ms DOUBLE, utc_offset_seconds INT,
  timezone STRING, timezone_abbreviation STRING, elevation DOUBLE,
  hourly_units MAP<STRING, STRING>,
  hourly STRUCT<
    time: ARRAY<STRING>, precipitation: ARRAY<DOUBLE>, rain: ARRAY<DOUBLE>,
    temperature_2m: ARRAY<DOUBLE>, relative_humidity_2m: ARRAY<DOUBLE>
  >
"""
SCHEMA_RELATOS = """
  id STRING, criado_em STRING, atualizado_em STRING, lat DOUBLE, lon DOUBLE,
  tipo_perigo STRING, severidade STRING, descricao STRING, status STRING,
  usuario_id STRING, origem STRING, versao_simulador STRING
"""


def carregar_incremental(pasta: str, padrao: str, schema: str, tabela: str) -> None:
    consulta = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{tabela}/schema")
        .option("rescuedDataColumn", "_rescued_data")
        .option("pathGlobFilter", padrao)
        .schema(schema)
        .load(f"{CHEGADA}/{pasta}/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_arquivo"),
            F.current_timestamp().alias("_carregado_em"),
        )
        .writeStream.option("checkpointLocation", f"{CHECKPOINTS}/{tabela}")
        .trigger(availableNow=True)  # processa só o que é novo e para
        .toTable(T(tabela))
    )
    consulta.awaitTermination()


carregar_incremental("clima", "clima_*.json", SCHEMA_CLIMA, "bronze_clima")
carregar_incremental("relatos", "relatos_*.json", SCHEMA_RELATOS, "bronze_relatos")

# COMMAND ----------

# Ciclovias: carga única (o GeoSampa não muda entre os lotes). Guardamos o GeoJSON
# inteiro como texto; a Prata faz o parse.
(
    spark.read.option("wholetext", True)
    .text(f"{CHEGADA}/ciclovias/")
    .select(
        F.col("value").alias("geojson"),
        F.col("_metadata.file_path").alias("_arquivo"),
        F.current_timestamp().alias("_carregado_em"),
    )
    .write.mode("overwrite")
    .saveAsTable(T("bronze_ciclovias"))
)

# COMMAND ----------

# MAGIC %md ### Contagens (item 05)

# COMMAND ----------

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {T('bronze_contagens')} (
    momento STRING, tabela STRING, linhas BIGINT, arquivos BIGINT, ultimo_arquivo STRING, registrado_em TIMESTAMP
  )
""")

contagens = None
for tabela in ["bronze_clima", "bronze_relatos"]:
    linha = spark.table(T(tabela)).select(
        F.lit(MOMENTO).alias("momento"),
        F.lit(tabela).alias("tabela"),
        F.count("*").alias("linhas"),
        F.countDistinct("_arquivo").alias("arquivos"),
        F.regexp_extract(F.max("_arquivo"), r"([^/]+)$", 1).alias("ultimo_arquivo"),
        F.current_timestamp().alias("registrado_em"),
    )
    contagens = linha if contagens is None else contagens.unionByName(linha)

contagens.write.mode("append").saveAsTable(T("bronze_contagens"))
display(spark.table(T("bronze_contagens")).orderBy("tabela", "registrado_em"))
