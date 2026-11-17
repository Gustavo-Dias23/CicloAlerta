"""Simulador de relatos de perigo do MVP (fonte "Relatos do MVP").

Por que simular: o endpoint GET /api/relatos do app ainda não está no ar. O
simulador gera o mesmo contrato que o endpoint vai expor, para o pipeline poder
rodar desde já. Quando o endpoint real subir, basta preencher a Variable
`gs_relatos_url` no Airflow e o DAG passa a baixar dele.

Como a geração funciona (documentado para o relatório):
- Determinística: a semente é a data. Rodar de novo para o mesmo dia gera
  exatamente o mesmo arquivo, então o pipeline é reprodutível.
- Volume: 30 a 45 relatos em dia útil, 15 a 25 no fim de semana.
- Horário: picos de deslocamento (7h-9h e 17h-19h), quase nada de madrugada.
- Local: ~85% dos relatos caem a até ~25 m de uma ciclovia real do GeoSampa
  (lida do arquivo de ciclovias em chegada/ciclovias/), o resto em pontos
  aleatórios da cidade (perigos fora da rede cicloviária).
- Atualizações: ~15% dos relatos do dia anterior reaparecem com novo status e
  novo `atualizado_em`, com o MESMO id. É isso que o MERGE da Prata atualiza.
- Erros de propósito (para as regras de qualidade terem o que barrar):
    * ~3% com coordenada fora da cidade de São Paulo;
    * ~3% com severidade fora da lista (ex.: "critica", "urgente", vazio, nulo);
    * ~4% com severidade só mal formatada ("Alta", " MEDIA ") - a Prata corrige
      com trim/lower, não barra;
    * ~2% de linhas duplicadas no mesmo arquivo (mesmo id) - a Prata deduplica.
- Pessoas: `usuario_id` é um pseudônimo fictício (usr_xxxxx) de um conjunto de
  400 usuários inventados. Nenhum dado pessoal real. Na Ouro ele vira hash.
- Limitação: o simulador NÃO correlaciona relatos com chuva. Qualquer relação
  chuva x relato encontrada nas consultas é coincidência até entrarem dados reais.
"""
from __future__ import annotations

import hashlib
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from gs_lib.geo import SP_LAT_MAX, SP_LAT_MIN, SP_LON_MAX, SP_LON_MIN, linhas_da_geometria

VERSAO_SIMULADOR = "1.0"

TIPOS_PERIGO = {
    # tipo: (peso, distribuição de severidade [baixa, media, alta])
    "buraco": (24, [0.35, 0.45, 0.20]),
    "obstrucao": (16, [0.30, 0.45, 0.25]),
    "conflito_veiculo": (14, [0.15, 0.40, 0.45]),
    "iluminacao_ruim": (12, [0.40, 0.45, 0.15]),
    "alagamento": (10, [0.15, 0.35, 0.50]),
    "piso_escorregadio": (10, [0.30, 0.45, 0.25]),
    "sinalizacao_apagada": (9, [0.50, 0.40, 0.10]),
}
SEVERIDADES = ["baixa", "media", "alta"]
SEVERIDADES_INVALIDAS = ["critica", "urgente", "", None, "5", "gravissima"]
STATUS_FINAIS = ["em_analise", "resolvido", "resolvido", "descartado"]

DESCRICOES = {
    "buraco": ["Buraco grande no meio da faixa", "Buraco perto da guia", "Asfalto afundado"],
    "obstrucao": ["Carro parado na ciclovia", "Entulho na ciclofaixa", "Caçamba bloqueando"],
    "conflito_veiculo": ["Ônibus invadindo a ciclofaixa", "Conversão sem sinalizar", "Moto na ciclovia"],
    "iluminacao_ruim": ["Trecho escuro à noite", "Poste apagado", "Iluminação fraca"],
    "alagamento": ["Poça grande depois da chuva", "Trecho alagado", "Bueiro entupido"],
    "piso_escorregadio": ["Tinta lisa molhada", "Óleo na pista", "Folhas acumuladas"],
    "sinalizacao_apagada": ["Pintura da faixa apagada", "Placa caída", "Sem tachões"],
}

# Pesos por hora (0-23): picos de deslocamento.
PESO_HORA = [1, 0.5, 0.3, 0.3, 0.5, 2, 5, 9, 9, 6, 4, 4, 5, 4, 4, 4, 5, 8, 9, 7, 5, 3, 2, 1]

TAXA_EM_CICLOVIA = 0.85
TAXA_FORA_SP = 0.03
TAXA_SEV_INVALIDA = 0.03
TAXA_SEV_MAL_FORMATADA = 0.04
TAXA_DUPLICADO = 0.02
TAXA_ATUALIZACAO_DIA_ANTERIOR = 0.15
N_USUARIOS = 400

# Usado só se o arquivo do GeoSampa ainda não existir (teste offline).
# Traçados aproximados de algumas ciclovias conhecidas, em [lon, lat].
CICLOVIAS_RESERVA = [
    [[-46.6610, -23.5570], [-46.6530, -23.5630], [-46.6430, -23.5716]],  # Av. Paulista
    [[-46.6930, -23.5670], [-46.6870, -23.5770], [-46.6810, -23.5870]],  # Av. Faria Lima
    [[-46.7430, -23.5340], [-46.7280, -23.5700], [-46.7160, -23.6200], [-46.7050, -23.6720]],  # Rio Pinheiros
    [[-46.6150, -23.5450], [-46.5700, -23.5420], [-46.5200, -23.5400]],  # Radial Leste
    [[-46.7300, -23.5750], [-46.7450, -23.5810], [-46.7600, -23.5880]],  # Eliseu de Almeida
    [[-46.6600, -23.5850], [-46.6615, -23.5950], [-46.6630, -23.6050]],  # República do Líbano
    [[-46.6440, -23.5480], [-46.6530, -23.5520], [-46.6610, -23.5560]],  # Consolação
    [[-46.6820, -23.5460], [-46.6795, -23.5380], [-46.6770, -23.5300]],  # Sumaré
]


def _rng(*partes: object) -> random.Random:
    semente = hashlib.sha256("|".join(map(str, partes)).encode()).hexdigest()
    return random.Random(int(semente[:16], 16))


def carregar_ciclovias(pasta_ciclovias: Path | None) -> list[list[list[float]]]:
    """Lê o GeoJSON mais recente do GeoSampa (já normalizado para lon/lat)."""
    if pasta_ciclovias and pasta_ciclovias.exists():
        arquivos = sorted(pasta_ciclovias.glob("ciclovias_*.geojson"))
        if arquivos:
            dados = json.loads(arquivos[-1].read_text(encoding="utf-8"))
            linhas: list[list[list[float]]] = []
            for feat in dados.get("features", []):
                for linha in linhas_da_geometria(feat.get("geometry") or {}):
                    if len(linha) >= 2:
                        linhas.append(linha)
            if linhas:
                return linhas
    return CICLOVIAS_RESERVA


def _segmentos(linhas: list[list[list[float]]]) -> tuple[list, list[float]]:
    segs, pesos = [], []
    for linha in linhas:
        for a, b in zip(linha, linha[1:]):
            comp = ((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2) ** 0.5
            if comp > 0:
                segs.append((a, b))
                pesos.append(comp)
    return segs, pesos


def _ponto_em_ciclovia(rng: random.Random, segs, pesos) -> tuple[float, float]:
    (x1, y1), (x2, y2) = rng.choices(segs, weights=pesos, k=1)[0]
    t = rng.random()
    ruido = 0.00008  # ~8 m de desvio padrão
    return x1 + t * (x2 - x1) + rng.gauss(0, ruido), y1 + t * (y2 - y1) + rng.gauss(0, ruido)


def _ponto_fora_de_sp(rng: random.Random) -> tuple[float, float]:
    opcoes = [
        (rng.uniform(-47.10, -46.95), rng.uniform(-22.95, -22.85)),  # Campinas
        (rng.uniform(-43.40, -43.20), rng.uniform(-22.98, -22.88)),  # Rio de Janeiro
        (0.0, 0.0),  # GPS sem sinal
        (rng.uniform(-23.70, -23.50), rng.uniform(-46.75, -46.55)),  # lat/lon trocados
    ]
    return rng.choice(opcoes)


def _relatos_base(dia: date, segs, pesos) -> list[dict]:
    """Relatos criados no dia (sem atualizações nem erros)."""
    rng = _rng("relatos", dia.isoformat())
    n = rng.randint(30, 45) if dia.weekday() < 5 else rng.randint(15, 25)
    tipos = list(TIPOS_PERIGO)
    pesos_tipo = [TIPOS_PERIGO[t][0] for t in tipos]
    relatos = []
    for i in range(n):
        hora = rng.choices(range(24), weights=PESO_HORA, k=1)[0]
        criado = datetime(dia.year, dia.month, dia.day, hora, rng.randint(0, 59), rng.randint(0, 59))
        if rng.random() < TAXA_EM_CICLOVIA:
            lon, lat = _ponto_em_ciclovia(rng, segs, pesos)
        else:
            lon, lat = rng.uniform(SP_LON_MIN + 0.08, SP_LON_MAX - 0.05), rng.uniform(SP_LAT_MIN + 0.25, SP_LAT_MAX - 0.03)
        tipo = rng.choices(tipos, weights=pesos_tipo, k=1)[0]
        severidade = rng.choices(SEVERIDADES, weights=TIPOS_PERIGO[tipo][1], k=1)[0]
        usuario = f"usr_{_rng('usuario', rng.randrange(N_USUARIOS)).getrandbits(20):05x}"
        relatos.append(
            {
                "id": f"R{dia:%Y%m%d}-{i + 1:03d}",
                "criado_em": criado.isoformat() + "-03:00",
                "atualizado_em": criado.isoformat() + "-03:00",
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "tipo_perigo": tipo,
                "severidade": severidade,
                "descricao": rng.choice(DESCRICOES[tipo]),
                "status": "aberto",
                "usuario_id": usuario,
                "origem": "simulador",
                "versao_simulador": VERSAO_SIMULADOR,
            }
        )
    return relatos


def gerar_relatos(dia: date | str, pasta_ciclovias: Path | None = None) -> list[dict]:
    """Relatos do dia `dia`, no mesmo formato que GET /api/relatos?data=AAAA-MM-DD."""
    if isinstance(dia, str):
        dia = date.fromisoformat(dia)
    segs, pesos = _segmentos(carregar_ciclovias(pasta_ciclovias))
    rng = _rng("ajustes", dia.isoformat())

    relatos = _relatos_base(dia, segs, pesos)

    # Atualizações de relatos de ontem (mesmo id, novo status) -> MERGE faz UPDATE.
    ontem = dia - timedelta(days=1)
    for r in _relatos_base(ontem, segs, pesos):
        if rng.random() < TAXA_ATUALIZACAO_DIA_ANTERIOR:
            r = dict(r)
            r["status"] = rng.choice(STATUS_FINAIS)
            r["atualizado_em"] = datetime(dia.year, dia.month, dia.day, rng.randint(8, 20), rng.randint(0, 59)).isoformat() + "-03:00"
            relatos.append(r)

    # Erros de propósito (só nos relatos novos do dia).
    for r in relatos:
        if not r["id"].startswith(f"R{dia:%Y%m%d}"):
            continue
        sorteio = rng.random()
        if sorteio < TAXA_FORA_SP:
            r["lon"], r["lat"] = (round(v, 6) for v in _ponto_fora_de_sp(rng))
        elif sorteio < TAXA_FORA_SP + TAXA_SEV_INVALIDA:
            r["severidade"] = rng.choice(SEVERIDADES_INVALIDAS)
        elif sorteio < TAXA_FORA_SP + TAXA_SEV_INVALIDA + TAXA_SEV_MAL_FORMATADA:
            r["severidade"] = rng.choice([r["severidade"].capitalize(), f" {r['severidade'].upper()} "])

    duplicados = [dict(r) for r in relatos if rng.random() < TAXA_DUPLICADO]
    relatos.extend(duplicados)
    relatos.sort(key=lambda r: r["atualizado_em"])
    return relatos
