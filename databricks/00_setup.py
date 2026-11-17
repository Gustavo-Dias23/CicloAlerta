# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Cria o schema `gs` e os volumes do pipeline. Rode **uma vez**, antes do upload do 1º lote.
# MAGIC
# MAGIC | Volume | Para quê |
# MAGIC | --- | --- |
# MAGIC | `chegada` | arquivos vindos do Airflow (subpastas `clima/`, `relatos/`, `ciclovias/`) |
# MAGIC | `checkpoints` | estado do Auto Loader (é o que impede reprocessar arquivo já lido) |
# MAGIC | `exportacao` | CSV da tabela de indicadores para o MVP |

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
CAT = dbutils.widgets.get("catalogo")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.gs COMMENT 'GS 2026.2 - perigos em ciclovias x chuva'")
for volume in ["chegada", "checkpoints", "exportacao"]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.gs.{volume}")

for pasta in ["clima", "relatos", "ciclovias"]:
    dbutils.fs.mkdirs(f"/Volumes/{CAT}/gs/chegada/{pasta}")

display(spark.sql(f"SHOW VOLUMES IN {CAT}.gs"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Próximo passo:** Catalog → `workspace` → `gs` → `chegada` → abra cada subpasta e use
# MAGIC *Upload to this volume* com os arquivos da pasta `chegada/` do Airflow (lote 1).
# MAGIC Depois rode `01_bronze`.
