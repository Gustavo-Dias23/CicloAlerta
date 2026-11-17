# GS 2026.2 · Componente Dados: perigos em ciclovias × chuva

**Integrantes**

| Nome | RM |
| --- | --- |
| Breno Silva | RM97864 |
| Enrico Marquez | RM99325 |
| Gustavo Dias | RM550820 |
| Joel Barros | RM550378 |
| Leonardo Moreira | RM550988 |


Pipeline que junta chuva por hora (Open-Meteo), a rede cicloviária (GeoSampa) e
os relatos de perigo do app, e entrega à tela **Indicadores** do MVP uma tabela pronta.

**Perguntas que o MVP responde**
1. Onde estão os perigos?
2. Quais ciclovias têm mais perigos?
3. Quanto dos relatos acontece com chuva?

## Arquitetura

```
Open-Meteo (API) ─┐
Relatos do MVP ───┼─► Airflow (gs_coleta, diário) ─► chegada/ ─► upload em lotes ─► Volume ─► Bronze (Auto Loader) ─► Prata (MERGE + qualidade) ─► Ouro (fato + dims, hash) ─► CSV ─► tela Indicadores
GeoSampa (WFS) ───┘   (gs_carga_ciclovias, 1 vez)
```

## Fontes

| Fonte | Tipo | Formato | Frequência | Arquivo em `chegada/` |
| --- | --- | --- | --- | --- |
| Open-Meteo, chuva por hora em SP | API pública real, sem chave | JSON | Diária | `clima/clima_AAAA-MM-DD.json` |
| Relatos do MVP (`GET /api/relatos`) | Dado do produto, **simulado e documentado** (ver abaixo) | JSON Lines | Diária | `relatos/relatos_AAAA-MM-DD.json` |
| Rede cicloviária do GeoSampa | Base aberta da Prefeitura (WFS) | GeoJSON | Carga única | `ciclovias/ciclovias_AAAA-MM-DD.geojson` |

### Por que a API de previsão do Open-Meteo, e não a de histórico
A `archive-api` (ERA5) publica cada dia com ~5 dias de atraso: a coleta de ontem
viria com chuva nula. A `api.open-meteo.com/v1/forecast` aceita `start_date`/`end_date`
nos últimos ~92 dias, sem atraso. Pedimos `timezone=America/Sao_Paulo` para as horas
baterem com as dos relatos.

### Relatos simulados
O endpoint do app ainda não está no ar. `dags/gs_lib/simulador_relatos.py` gera o
mesmo contrato do endpoint. A geração é **determinística** (semente = data), segue
picos de horário de deslocamento e põe ~85% dos pontos a até ~25 m de ciclovias reais
do GeoSampa. ~15% dos relatos de ontem reaparecem com novo status (é o que o MERGE
atualiza). Para as regras de qualidade terem o que barrar, entram **erros de propósito**:
~3% de coordenadas fora de SP, ~3% de severidades inválidas, ~4% de severidades só mal
formatadas (corrigidas na Prata) e ~2% de linhas duplicadas. `usuario_id` é um pseudônimo
inventado, e não há nenhum dado pessoal real. **Limitação:** o simulador não correlaciona
relatos com chuva. Quando o endpoint real existir, basta preencher a Variable
`gs_relatos_url`.

Contrato de um relato:
```json
{"id": "R20261115-003", "criado_em": "2026-11-15T00:08:14-03:00", "atualizado_em": "2026-11-15T00:08:14-03:00",
 "lat": -23.56672, "lon": -46.648786, "tipo_perigo": "buraco", "severidade": "alta",
 "descricao": "Buraco perto da guia", "status": "aberto", "usuario_id": "usr_c801e",
 "origem": "simulador", "versao_simulador": "1.0"}
```
`tipo_perigo` ∈ buraco, obstrucao, conflito_veiculo, iluminacao_ruim, alagamento, piso_escorregadio, sinalizacao_apagada ·
`severidade` ∈ baixa, media, alta · `status` ∈ aberto, em_analise, resolvido, descartado

## Estrutura

```
dags/
  gs_coleta.py            DAG diário: inicio >> [coletar_clima, coletar_relatos] >> conferir_chegada >> notificar
  gs_carga_ciclovias.py   DAG manual: baixar_ciclovias >> conferir_ciclovias
  gs_lib/comum.py         pasta chegada/, leitura de Variables, notificação (webhook)
  gs_lib/simulador_relatos.py
  gs_lib/geo.py           caixa de SP, UTM 23S -> lon/lat, haversine
simulador/api_relatos.py  API fake do GET /api/relatos com token (opcional)
databricks/               notebooks Bronze, Prata, Ouro e as 5 consultas
amostras/                 um pouco de cada fonte
```

## Como reproduzir

### 1. Airflow (3.x)
1. Copie a pasta `dags/` (com `gs_lib/`) para a pasta de DAGs do Airflow.
2. **Docker Compose oficial:** em `x-airflow-common > volumes`, acrescente
   `- ${AIRFLOW_PROJ_DIR:-.}/chegada:/opt/airflow/chegada` e reinicie. Sem isso os
   arquivos ficam presos dentro do container. (Fora do Docker, `chegada/` é criada na raiz do repositório.)
3. Variables (Admin > Variables). Todas são opcionais:

   | Variable | Para quê |
   | --- | --- |
   | `gs_relatos_url` | URL do `GET /api/relatos`. Vazia = simulador local |
   | `gs_relatos_token` | token Bearer do endpoint (segredo, nunca no código) |
   | `gs_webhook_url` | webhook Slack/Discord para avisos de falha e sucesso |
   | `gs_pasta_chegada` | outro caminho para `chegada/` |
   | `gs_ciclovias_url` | GeoJSON de ciclovias, se a descoberta automática no WFS falhar |

4. Rode **primeiro** o `gs_carga_ciclovias` (Trigger). Os relatos simulados usam esse
   arquivo para cair em cima das ciclovias reais.
5. Ative (unpause) o `gs_coleta`. Com `catchup=True` ele gera os dias desde 01/11, e daí
   em diante roda todo dia à 00h de São Paulo, coletando o dia anterior.

Notificação: toda tarefa que falha chama `avisar_falha`, e a tarefa `notificar` avisa o
sucesso com o resumo da conferência. Os dois vão para o log e, se houver, para o webhook.

### 2. Databricks (Free Edition, serverless)
Importe os notebooks de `databricks/` (Workspace → Import, um arquivo por vez) e rode nesta ordem:

| Notebook | O que faz |
| --- | --- |
| `00_setup` | cria o schema `gs` e os volumes `chegada`, `checkpoints` e `exportacao` |
| `01_bronze` | Auto Loader (`availableNow`) → `bronze_clima`, `bronze_relatos`, `bronze_ciclovias`; grava as contagens em `bronze_contagens` conforme o widget `momento` |
| `02_prata` | `arrays_zip` + `explode` no clima, tipos, 3 regras de qualidade (`prata_qualidade`), MERGE por `hora` e por `id`, segmentos das ciclovias |
| `03_ouro` | `dim_tempo`, `dim_ciclovia`, `dim_tipo_perigo`, `fato_relato` (com `usuario_hash`), `ouro_indicadores` e o CSV em `exportacao/` |
| `04_consultas` | as 5 consultas SQL |

Lotes: o lote 1 são os arquivos de 01/11 a 15/11 e o lote 2 é o de 16/11 (dias de coleta diferentes).
Sequência: upload do lote 1 → `01_bronze` (momento 1) → upload do lote 2 → `01_bronze` (momento 2) →
`01_bronze` de novo, sem arquivo novo (momento 3) → `02_prata` → `03_ouro` → `04_consultas`.

**Relato → ciclovia:** a menor distância do ponto até um segmento de ciclovia (projeção plana local). Até 30 m o
relato conta como "na ciclovia"; acima disso entra como `-1 · Fora da rede cicloviária`.
**Hora de chuva:** precipitação daquela hora ≥ 0,1 mm. **Alerta:** severidade alta + chuva + dentro da rede.

## LGPD
Na Ouro, `usuario_id` é trocado por `sha2(concat(usuario_id, salt), 256)`, com o salt fora
do repositório: num secret `gs/salt_lgpd` ou digitado no widget `salt_lgpd` do
`03_ouro`. Usem sempre o mesmo valor e não mostrem o widget nos prints. O repositório só tem dados simulados.
