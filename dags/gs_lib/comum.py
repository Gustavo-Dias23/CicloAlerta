"""Configuração compartilhada dos DAGs: pasta chegada/, Variables e notificação.

Todas as Variables são lidas DENTRO das tarefas (nunca no topo do arquivo), para
o scheduler não consultar o banco a cada parse do DAG.

Variables usadas (Admin > Variables). Nenhuma é obrigatória:
  gs_pasta_chegada   caminho da pasta chegada/   (padrão: <raiz do repo>/chegada)
  gs_relatos_url     URL do GET /api/relatos     (vazio = usa o simulador local)
  gs_relatos_token   token Bearer do endpoint    (segredo: nunca no código)
  gs_clima_url       endpoint do Open-Meteo      (padrão: API de previsão)
  gs_webhook_url     webhook Slack/Discord p/ avisos (vazio = só log)
  gs_ciclovias_url   URL completa de um GeoJSON de ciclovias (sobrescreve a descoberta no WFS)
  gs_wfs_geosampa    endpoint WFS do GeoSampa    (padrão: wfs.geosampa.prefeitura.sp.gov.br)
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import requests

log = logging.getLogger(__name__)

RAIZ_REPO = Path(__file__).resolve().parents[2]


def variavel(chave: str, padrao: str | None = None) -> str | None:
    """Lê uma Airflow Variable; fora do Airflow (testes) cai em variável de ambiente."""
    try:
        from airflow.sdk import Variable

        valor = Variable.get(chave, default=None)
    except Exception:  # noqa: BLE001 - fora do Airflow ou sem banco
        valor = None
    if valor in (None, ""):
        valor = os.environ.get(chave.upper(), padrao)
    return valor if valor not in ("",) else padrao


def pasta_chegada() -> Path:
    pasta = Path(variavel("gs_pasta_chegada", str(RAIZ_REPO / "chegada")))
    pasta.mkdir(parents=True, exist_ok=True)
    return pasta


def notificar(mensagem: str) -> None:
    """Loga sempre; se houver webhook configurado, envia também."""
    log.info("[gs] %s", mensagem)
    url = variavel("gs_webhook_url")
    if not url:
        return
    try:
        # "text" = Slack/Teams (webhook simples), "content" = Discord.
        requests.post(url, json={"text": mensagem, "content": mensagem}, timeout=10)
    except requests.RequestException as erro:
        log.warning("Falha ao enviar notificação: %s", erro)


def avisar_falha(context) -> None:
    """on_failure_callback: avisa qual tarefa falhou, em qual data e o erro."""
    ti = context.get("task_instance") or context.get("ti")
    tarefa = getattr(ti, "task_id", "?")
    dag_id = getattr(ti, "dag_id", "?")
    notificar(f"FALHA em {dag_id}.{tarefa} (data {context.get('ds')}): {context.get('exception')}")
