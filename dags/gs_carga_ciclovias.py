"""DAG gs_carga_ciclovias: carga única da rede cicloviária do GeoSampa.

Roda manualmente (schedule=None), uma vez, antes do 1º lote. Grava:
  chegada/ciclovias/ciclovias_AAAA-MM-DD.geojson   (FeatureCollection em lon/lat)

Como acha a camada: lê o GetCapabilities do WFS do GeoSampa e escolhe a camada
cuja descrição começa com "Rede cicloviária municipal". Se a Prefeitura mudar o
nome, preencha a Variable `gs_ciclovias_url` com a URL completa de um GeoJSON
(ou baixe o arquivo à mão e coloque na pasta acima com esse padrão de nome).

O GeoSampa publica em SIRGAS 2000 / UTM 23S (EPSG:31983). Pedimos EPSG:4326 ao
servidor e, por garantia, normalizamos as coordenadas aqui (gs_lib.geo), para a
Bronze já receber lon/lat em graus.
"""
from __future__ import annotations

import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import timedelta

import pendulum
import requests
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from gs_lib.comum import avisar_falha, notificar, pasta_chegada, variavel
from gs_lib.geo import dentro_de_sp, linhas_da_geometria, normalizar_geometria

log = logging.getLogger(__name__)

WFS_GEOSAMPA = "http://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs"
DESCRICAO_CAMADA = r"rede ciclovi[áa]ria municipal"


def descobrir_camada() -> str:
    resp = requests.get(
        variavel("gs_wfs_geosampa", WFS_GEOSAMPA),
        params={"service": "WFS", "version": "2.0.0", "request": "GetCapabilities"},
        timeout=60,
    )
    resp.raise_for_status()
    raiz = ET.fromstring(resp.content)

    def local(el) -> str:  # nome da tag sem namespace
        return el.tag.rsplit("}", 1)[-1]

    def filho(el, nome: str) -> str:
        return next((c.text or "" for c in el if local(c) == nome), "")

    candidatas = []
    for ft in (el for el in raiz.iter() if local(el) == "FeatureType"):
        nome = filho(ft, "Name").strip()
        texto = f"{filho(ft, 'Title')} {filho(ft, 'Abstract')}".lower()
        if re.search(DESCRICAO_CAMADA, texto) and "sac_" not in nome:
            candidatas.append(nome)
    if not candidatas:
        raise ValueError("Camada da rede cicloviária não encontrada no WFS. Preencha a Variable gs_ciclovias_url.")
    log.info("Camadas candidatas: %s (usando a primeira)", candidatas)
    return candidatas[0]


def baixar_ciclovias(ds: str, **_) -> str:
    url = variavel("gs_ciclovias_url")
    if url:
        resp = requests.get(url, timeout=120)
    else:
        camada = descobrir_camada()
        resp = requests.get(
            variavel("gs_wfs_geosampa", WFS_GEOSAMPA),
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": camada,
                "outputFormat": "application/json",
                "srsName": "EPSG:4326",
            },
            timeout=180,
        )
    resp.raise_for_status()
    dados = resp.json()

    for feat in dados.get("features", []):
        feat["geometry"] = normalizar_geometria(feat.get("geometry") or {})
    dados.pop("crs", None)  # GeoJSON padrão (RFC 7946) já é WGS84 lon/lat

    destino = pasta_chegada() / "ciclovias" / f"ciclovias_{ds}.geojson"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(json.dumps(dados, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return str(destino)


def conferir_ciclovias(ti, **_) -> None:
    caminho = ti.xcom_pull(task_ids="baixar_ciclovias")
    dados = json.loads(open(caminho, encoding="utf-8").read())
    feats = dados.get("features", [])
    if not feats:
        raise ValueError("GeoJSON de ciclovias sem features")
    pontos = [p for f in feats for linha in linhas_da_geometria(f["geometry"]) for p in linha]
    dentro = sum(dentro_de_sp(lat, lon) for lon, lat in pontos)
    if dentro / max(len(pontos), 1) < 0.9:
        raise ValueError(f"Só {dentro}/{len(pontos)} vértices dentro de SP: coordenadas em sistema errado?")
    notificar(f"gs_carga_ciclovias OK: {len(feats)} trechos, {len(pontos)} vértices em {caminho}")


with DAG(
    dag_id="gs_carga_ciclovias",
    description="Carga única da rede cicloviária do GeoSampa -> chegada/ciclovias/",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1), "on_failure_callback": avisar_falha},
    tags=["gs", "coleta", "carga_unica"],
) as dag:
    baixar = PythonOperator(task_id="baixar_ciclovias", python_callable=baixar_ciclovias)
    conferir = PythonOperator(task_id="conferir_ciclovias", python_callable=conferir_ciclovias)
    baixar >> conferir
