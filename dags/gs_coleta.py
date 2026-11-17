"""DAG gs_coleta: coleta diária de chuva (Open-Meteo) e relatos do MVP.

Grava em chegada/ um arquivo por coleta, com a data no nome:
  chegada/clima/clima_AAAA-MM-DD.json      resposta do Open-Meteo (horária, 24 h)
  chegada/relatos/relatos_AAAA-MM-DD.json  um relato por linha (JSON Lines)

Fluxo:  inicio >> [coletar_clima, coletar_relatos] >> conferir_chegada >> notificar
Agendado à meia-noite de São Paulo: a execução de "ds" roda depois que o dia
"ds" acabou e coleta o dia inteiro.
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from gs_lib.comum import avisar_falha, notificar, pasta_chegada, variavel

log = logging.getLogger(__name__)

# Ponto central de São Paulo (Sé). Uma série de chuva para a cidade toda.
LATITUDE, LONGITUDE = -23.55, -46.63
FUSO = "America/Sao_Paulo"
# API de previsão: tem os últimos ~92 dias sem atraso. A archive-api (ERA5)
# só publica com ~5 dias de atraso e devolveria chuva nula para ontem.
CLIMA_URL_PADRAO = "https://api.open-meteo.com/v1/forecast"
VARIAVEIS_HORARIAS = "precipitation,rain,temperature_2m,relative_humidity_2m"


def coletar_clima(ds: str, **_) -> str:
    resposta = requests.get(
        variavel("gs_clima_url", CLIMA_URL_PADRAO),
        params={
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": ds,
            "end_date": ds,
            "hourly": VARIAVEIS_HORARIAS,
            "timezone": FUSO,  # horas no horário local, iguais às dos relatos
        },
        timeout=30,
    )
    resposta.raise_for_status()
    dados = resposta.json()
    if "hourly" not in dados:
        raise ValueError(f"Resposta do Open-Meteo sem 'hourly': {str(dados)[:200]}")

    destino = pasta_chegada() / "clima" / f"clima_{ds}.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    # Uma linha só: o Auto Loader lê sem precisar de multiLine.
    destino.write_text(json.dumps(dados, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return str(destino)


def coletar_relatos(ds: str, **_) -> str:
    url = variavel("gs_relatos_url")
    if url:
        token = variavel("gs_relatos_token")
        cabecalhos = {"Authorization": f"Bearer {token}"} if token else {}
        resposta = requests.get(url, params={"data": ds}, headers=cabecalhos, timeout=30)
        resposta.raise_for_status()
        corpo = resposta.json()
        relatos = corpo["relatos"] if isinstance(corpo, dict) else corpo
        origem = url
    else:
        from gs_lib.simulador_relatos import gerar_relatos

        relatos = gerar_relatos(ds, pasta_chegada() / "ciclovias")
        origem = "simulador local (Variable gs_relatos_url vazia)"

    destino = pasta_chegada() / "relatos" / f"relatos_{ds}.json"
    destino.parent.mkdir(parents=True, exist_ok=True)
    # JSON Lines: um relato por linha -> na Bronze, uma linha por relato.
    with destino.open("w", encoding="utf-8") as f:
        for relato in relatos:
            f.write(json.dumps(relato, ensure_ascii=False) + "\n")
    log.info("relatos %s: %d registros de %s", ds, len(relatos), origem)
    return str(destino)


def conferir_chegada(ds: str, **_) -> dict:
    """Falha se algum arquivo do dia não existir, estiver vazio ou mal formado."""
    base = pasta_chegada()
    clima = base / "clima" / f"clima_{ds}.json"
    relatos = base / "relatos" / f"relatos_{ds}.json"
    problemas = []

    for arq in (clima, relatos):
        if not arq.exists():
            problemas.append(f"{arq.name} não existe")
        elif arq.stat().st_size == 0:
            problemas.append(f"{arq.name} está vazio")

    horas = n_relatos = 0
    if not problemas:
        horario = json.loads(clima.read_text(encoding="utf-8"))["hourly"]
        horas = len(horario.get("time", []))
        if horas != 24:
            problemas.append(f"{clima.name} tem {horas} horas (esperado 24)")
        if all(v is None for v in horario.get("precipitation", [])):
            problemas.append(f"{clima.name} veio com precipitação toda nula")
        linhas = relatos.read_text(encoding="utf-8").splitlines()
        n_relatos = len(linhas)
        for i, linha in enumerate(linhas, 1):
            try:
                json.loads(linha)
            except json.JSONDecodeError:
                problemas.append(f"{relatos.name} linha {i} não é JSON válido")
                break

    if problemas:
        raise ValueError("; ".join(problemas))
    resumo = {
        "data": ds,
        "clima": {"arquivo": clima.name, "bytes": clima.stat().st_size, "horas": horas},
        "relatos": {"arquivo": relatos.name, "bytes": relatos.stat().st_size, "linhas": n_relatos},
    }
    return resumo


def notificar_sucesso(ds: str, ti, **_) -> None:
    r = ti.xcom_pull(task_ids="conferir_chegada")
    notificar(
        f"gs_coleta {ds} OK: {r['clima']['arquivo']} ({r['clima']['horas']} h), "
        f"{r['relatos']['arquivo']} ({r['relatos']['linhas']} relatos)"
    )


with DAG(
    dag_id="gs_coleta",
    description="Coleta diária: chuva (Open-Meteo) + relatos do MVP -> chegada/",
    # Intervalo explícito: a execução de "ds" roda à 00h do dia seguinte e coleta o
    # dia "ds" inteiro. (No Airflow 3 um cron simples usa CronTriggerTimetable, que
    # faria ds = dia de hoje, ainda incompleto.)
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=FUSO),
    start_date=pendulum.datetime(2026, 11, 1, tz=FUSO),
    catchup=True,  # gera também os dias desde 01/11 (histórico para a Bronze)
    max_active_runs=4,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": avisar_falha,
    },
    tags=["gs", "coleta"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    clima = PythonOperator(task_id="coletar_clima", python_callable=coletar_clima)
    relatos = PythonOperator(task_id="coletar_relatos", python_callable=coletar_relatos)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada, retries=0)
    aviso = PythonOperator(task_id="notificar", python_callable=notificar_sucesso)

    inicio >> [clima, relatos] >> conferir >> aviso
