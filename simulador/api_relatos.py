"""API fake do GET /api/relatos (opcional), com o mesmo contrato do endpoint do MVP.

Serve para testar o modo "API" do DAG (URL + token em Variables) antes de o
endpoint real existir. Só usa a biblioteca padrão.

    RELATOS_TOKEN=troque-me python simulador/api_relatos.py      # porta 8085
    curl -H "Authorization: Bearer troque-me" "localhost:8085/api/relatos?data=2026-11-15"

No Airflow: gs_relatos_url = http://host.docker.internal:8085/api/relatos
            gs_relatos_token = o mesmo valor de RELATOS_TOKEN
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))
from gs_lib.simulador_relatos import gerar_relatos  # noqa: E402

TOKEN = os.environ.get("RELATOS_TOKEN")
PASTA_CICLOVIAS = Path(os.environ.get("GS_PASTA_CHEGADA", RAIZ / "chegada")) / "ciclovias"


class Handler(BaseHTTPRequestHandler):
    def _responder(self, status: int, corpo: object) -> None:
        dados = json.dumps(corpo, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(dados)))
        self.end_headers()
        self.wfile.write(dados)

    def do_GET(self) -> None:  # noqa: N802
        url = urlparse(self.path)
        if url.path != "/api/relatos":
            return self._responder(404, {"erro": "rota inexistente"})
        if TOKEN and self.headers.get("Authorization") != f"Bearer {TOKEN}":
            return self._responder(401, {"erro": "token inválido"})
        try:
            dia = date.fromisoformat(parse_qs(url.query).get("data", [date.today().isoformat()])[0])
        except ValueError:
            return self._responder(400, {"erro": "use ?data=AAAA-MM-DD"})
        relatos = gerar_relatos(dia, PASTA_CICLOVIAS)
        self._responder(200, {"data": dia.isoformat(), "total": len(relatos), "relatos": relatos})


if __name__ == "__main__":
    porta = int(os.environ.get("PORTA", "8085"))
    print(f"API de relatos simulada em http://0.0.0.0:{porta}/api/relatos (token {'ligado' if TOKEN else 'desligado'})")
    ThreadingHTTPServer(("0.0.0.0", porta), Handler).serve_forever()
