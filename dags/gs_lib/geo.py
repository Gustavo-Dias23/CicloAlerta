"""Utilidades geográficas sem dependências externas (roda no container do Airflow).

- Caixa da cidade de São Paulo (usada no simulador e na regra de qualidade da Prata).
- Conversão UTM 23S (SIRGAS 2000 / EPSG:31983) -> lat/lon, porque o GeoSampa
  publica as camadas nesse sistema. SIRGAS 2000 e WGS84 diferem em centímetros,
  irrelevante para associar relatos a ciclovias.
"""
from __future__ import annotations

import math

# Caixa envolvente do município de São Paulo (aprox.). Mesma faixa usada na Prata.
SP_LAT_MIN, SP_LAT_MAX = -24.01, -23.35
SP_LON_MIN, SP_LON_MAX = -46.83, -46.36


def dentro_de_sp(lat: float, lon: float) -> bool:
    return SP_LAT_MIN <= lat <= SP_LAT_MAX and SP_LON_MIN <= lon <= SP_LON_MAX


def utm_para_lonlat(easting: float, northing: float, zona: int = 23, sul: bool = True) -> tuple[float, float]:
    """Inversa da projeção UTM (elipsoide GRS80). Precisão de centímetros."""
    a = 6378137.0
    f = 1 / 298.257222101
    k0 = 0.9996
    e2 = f * (2 - f)
    ep2 = e2 / (1 - e2)

    x = easting - 500000.0
    y = northing - (10000000.0 if sul else 0.0)

    m = y / k0
    mu = m / (a * (1 - e2 / 4 - 3 * e2**2 / 64 - 5 * e2**3 / 256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi1 = (
        mu
        + (3 * e1 / 2 - 27 * e1**3 / 32) * math.sin(2 * mu)
        + (21 * e1**2 / 16 - 55 * e1**4 / 32) * math.sin(4 * mu)
        + (151 * e1**3 / 96) * math.sin(6 * mu)
        + (1097 * e1**4 / 512) * math.sin(8 * mu)
    )
    n1 = a / math.sqrt(1 - e2 * math.sin(phi1) ** 2)
    t1 = math.tan(phi1) ** 2
    c1 = ep2 * math.cos(phi1) ** 2
    r1 = a * (1 - e2) / (1 - e2 * math.sin(phi1) ** 2) ** 1.5
    d = x / (n1 * k0)

    lat = phi1 - (n1 * math.tan(phi1) / r1) * (
        d**2 / 2
        - (5 + 3 * t1 + 10 * c1 - 4 * c1**2 - 9 * ep2) * d**4 / 24
        + (61 + 90 * t1 + 298 * c1 + 45 * t1**2 - 252 * ep2 - 3 * c1**2) * d**6 / 720
    )
    lon0 = math.radians((zona - 1) * 6 - 180 + 3)
    lon = lon0 + (
        d
        - (1 + 2 * t1 + c1) * d**3 / 6
        + (5 - 2 * c1 + 28 * t1 - 3 * c1**2 + 8 * ep2 + 24 * t1**2) * d**5 / 120
    ) / math.cos(phi1)
    return math.degrees(lon), math.degrees(lat)


def normalizar_par(p: list[float]) -> list[float]:
    """Devolve [lon, lat] em graus, venha o ponto em UTM, lat/lon ou lon/lat."""
    x, y = float(p[0]), float(p[1])
    if abs(x) > 1000 or abs(y) > 1000:  # metros -> UTM 23S
        lon, lat = utm_para_lonlat(x, y)
        return [round(lon, 7), round(lat, 7)]
    # Em SP, |lon| ~ 46 e |lat| ~ 23. Se vier invertido (eixo lat/lon), troca.
    if abs(x) < 30 and abs(y) > 40:
        x, y = y, x
    return [round(x, 7), round(y, 7)]


def normalizar_geometria(geom: dict) -> dict:
    """Converte LineString/MultiLineString para [lon, lat] em graus."""
    tipo = geom.get("type")
    coords = geom.get("coordinates") or []
    if tipo == "LineString":
        return {"type": tipo, "coordinates": [normalizar_par(p) for p in coords]}
    if tipo == "MultiLineString":
        return {"type": tipo, "coordinates": [[normalizar_par(p) for p in linha] for linha in coords]}
    return geom


def linhas_da_geometria(geom: dict) -> list[list[list[float]]]:
    tipo = geom.get("type")
    if tipo == "LineString":
        return [geom["coordinates"]]
    if tipo == "MultiLineString":
        return list(geom["coordinates"])
    return []


def distancia_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Haversine em metros."""
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(h))
