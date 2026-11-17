-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · As 5 consultas (item 07)
-- MAGIC Rodam sobre a Ouro. Embaixo de cada uma há a interpretação dos resultados da execução de 17/11/2026
-- MAGIC (lote 1: 01 a 15/11; lote 2: 16/11).

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA gs;

-- COMMAND ----------

-- MAGIC %md ### 1. Ciclovias com mais perigos
-- MAGIC Agrupa por nome (o GeoSampa divide a mesma via em vários trechos) e normaliza por km,
-- MAGIC para uma ciclovia longa não parecer pior só por ser longa.

-- COMMAND ----------

WITH km AS (
  SELECT nome, SUM(comprimento_km) AS km
  FROM dim_ciclovia
  WHERE ciclovia_id <> '-1'
  GROUP BY nome
)
SELECT
  c.nome AS ciclovia,
  COUNT(*) AS perigos,
  SUM(CASE WHEN f.severidade = 'alta' THEN 1 ELSE 0 END) AS perigos_alta,
  ROUND(MAX(km.km), 2) AS extensao_km,
  ROUND(COUNT(*) / NULLIF(MAX(km.km), 0), 1) AS perigos_por_km
FROM fato_relato f
JOIN dim_ciclovia c ON f.ciclovia_id = c.ciclovia_id
JOIN km ON km.nome = c.nome
WHERE f.ciclovia_id <> '-1' AND f.status <> 'descartado'
GROUP BY c.nome
ORDER BY perigos DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md **Interpretação:** em número absoluto, Caminho Verde, Rio Pinheiros e Assis Ribeiro lideram, com 8 perigos cada. Mas Rio Pinheiros e Assis Ribeiro estão entre as mais longas (22 e 23 km). Por km, os trechos mais críticos são a Ciclofaixa Parque Arariba (1,0 perigo/km) e a Ciclorrota Lapa (0,9/km). Por isso, a prioridade de manutenção deve olhar perigos por km, e não o total.

-- COMMAND ----------

-- MAGIC %md ### 2. Relatos por tipo e severidade

-- COMMAND ----------

SELECT
  t.descricao AS tipo_perigo,
  t.categoria,
  SUM(CASE WHEN f.severidade = 'baixa' THEN 1 ELSE 0 END) AS baixa,
  SUM(CASE WHEN f.severidade = 'media' THEN 1 ELSE 0 END) AS media,
  SUM(CASE WHEN f.severidade = 'alta'  THEN 1 ELSE 0 END) AS alta,
  COUNT(*) AS total,
  ROUND(100 * SUM(CASE WHEN f.severidade = 'alta' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_alta
FROM fato_relato f
JOIN dim_tipo_perigo t ON f.tipo_perigo_id = t.tipo_perigo_id
WHERE f.status <> 'descartado'
GROUP BY t.descricao, t.categoria
ORDER BY total DESC;

-- COMMAND ----------

-- MAGIC %md **Interpretação:** buraco é o perigo mais relatado: 122 dos 463 relatos (26%). Já o alagamento é o mais grave: 58,5% dos casos são de severidade alta, seguido de conflito com veículo (39,7%). Iluminação ruim quase nunca é grave (7%).

-- COMMAND ----------

-- MAGIC %md ### 3. Parte dos relatos feita em hora de chuva
-- MAGIC Compara com a parte das horas que teve chuva no período. Se os relatos em chuva
-- MAGIC passam muito da parte de horas chuvosas, a chuva aumenta os relatos.

-- COMMAND ----------

SELECT
  COUNT(*) AS relatos,
  SUM(CAST(f.em_chuva AS INT)) AS relatos_em_chuva,
  ROUND(100 * AVG(CAST(f.em_chuva AS INT)), 1) AS pct_relatos_em_chuva,
  (SELECT ROUND(100 * AVG(CAST(hora_com_chuva AS INT)), 1) FROM dim_tempo WHERE precipitacao_mm IS NOT NULL) AS pct_horas_com_chuva
FROM fato_relato f
WHERE f.status <> 'descartado';

-- COMMAND ----------

-- MAGIC %md **Interpretação:** 142 dos 463 relatos (30,7%) aconteceram em hora de chuva, exatamente a mesma proporção das horas chuvosas no período (30,7%, dado real do Open-Meteo). Ou seja, nestes dados a chuva não aumentou o número de relatos. Isso já era esperado, porque o simulador não relaciona relatos com chuva. Com os relatos reais do app, esta consulta vai mostrar se a chuva aumenta os perigos.

-- COMMAND ----------

-- MAGIC %md ### 4. Horários com mais relatos

-- COMMAND ----------

SELECT
  d.hora,
  d.periodo,
  COUNT(*) AS relatos,
  SUM(CASE WHEN f.severidade = 'alta' THEN 1 ELSE 0 END) AS relatos_alta
FROM fato_relato f
JOIN dim_tempo d ON f.sk_tempo = d.sk_tempo
WHERE f.status <> 'descartado'
GROUP BY d.hora, d.periodo
ORDER BY relatos DESC
LIMIT 8;

-- COMMAND ----------

-- MAGIC %md **Interpretação:** os relatos se concentram no horário de ida ao trabalho (7h: 39; 8h: 34) e no de volta (17h a 20h: de 31 a 34 por hora). Às 8h fica o maior número de relatos de severidade alta (12). É nesses horários que um alerta ao ciclista tem mais efeito.

-- COMMAND ----------

-- MAGIC %md ### 5. Impacto: perigos de severidade alta em ciclovias durante a chuva
-- MAGIC São os casos que geram alerta ao ciclista (`alerta_ciclista`). A linha TOTAL soma todas as ciclovias.

-- COMMAND ----------

SELECT
  COALESCE(c.nome, 'TOTAL') AS ciclovia,
  COUNT(*) AS alertas,
  ROUND(AVG(f.precipitacao_mm), 1) AS chuva_media_mm,
  ROUND(MAX(f.precipitacao_mm), 1) AS chuva_max_mm
FROM fato_relato f
JOIN dim_ciclovia c ON f.ciclovia_id = c.ciclovia_id
WHERE f.alerta_ciclista AND f.status <> 'descartado'
GROUP BY ROLLUP (c.nome)
ORDER BY CASE WHEN ciclovia = 'TOTAL' THEN 0 ELSE 1 END, alertas DESC;

-- COMMAND ----------

-- MAGIC %md **Interpretação:** 29 perigos de severidade alta aconteceram em ciclovias durante a chuva, espalhados por 27 ciclovias; nenhuma passa de 2 alertas (Vila Andrade / Vila Sônia e Caetano Álvares). São esses os casos que o app mostraria como alerta ao ciclista. A chuva média nesses casos foi de só 0,7 mm, e 25 dos 29 aconteceram com menos de 1 mm. Se o alerta só disparasse a partir de 1 mm/h, o número cairia para 4, que são os casos de chuva forte de verdade (Vila Mariana 5,8 mm, Carvalho Pinto 4,2 mm, Estado 2,3 mm e Vila Andrade / Vila Sônia 1,0 mm). Isso evita que o ciclista receba tantos alertas a ponto de passar a ignorá-los.