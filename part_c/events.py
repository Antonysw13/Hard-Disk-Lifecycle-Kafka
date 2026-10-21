"""Lo que comparten el producer y el job de streaming: formato de los eventos y ventanas.

Cada evento es una fila de Backblaze (un disco en un día) en JSON:
    {"date": "2026-04-01", "serial_number": "ZLW1AHGN", "model": "ST12000NM001G", "failure": 0}

El tiempo del evento es `date`, no el momento en que llega a Kafka: las ventanas y el
watermark trabajan sobre ese tiempo.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import zlib

EVENT_SCHEMA = "date STRING, serial_number STRING, model STRING, failure INT"

# Marca de fin de flujo: un evento sin datos con una fecha posterior a la última ventana más el
# watermark. Hace avanzar el watermark para que Spark cierre (y emita) las últimas ventanas.
END_MODEL = "__fin__"

# 5 modelos con muchos discos y AFR distintas en la muestra de abril de 2026 (Parte A),
# unos 120.000 eventos por día: el enunciado pide filtrar de 3 a 5 modelos.
DEFAULT_MODELS = (
    "WDC WUH722222ALE6L4",
    "TOSHIBA MG07ACA14TA",
    "ST12000NM0008",
    "HGST HUH721212ALN604",
    "ST24000NM002H",
)

EPOCH = dt.date(1970, 1, 1)
LATE_SALT = "late:"


_TEMPLATE = '{"date":"%s","serial_number":"%s","model":"%s","failure":%d}'
_NEEDS_ESCAPE = re.compile(r'["\\\x00-\x1f]')


def encode(date: str, serial: str, model: str, failure: int) -> bytes:
    """JSON del evento. Con texto sin comillas ni barras (lo normal) basta una plantilla, que
    es varias veces más rápida que json.dumps; si no, se usa json.dumps."""
    if _NEEDS_ESCAPE.search(date + serial + model) is None:
        return (_TEMPLATE % (date, serial, model, failure)).encode()
    event = {"date": date, "serial_number": serial, "model": model, "failure": failure}
    return json.dumps(event, separators=(",", ":")).encode()


def end_marker(date: dt.date) -> bytes:
    return encode(date.isoformat(), "", END_MODEL, 0)


def in_bucket(serial: str, pct: float, salt: str = "") -> bool:
    """Selección reproducible de un `pct` % de los discos por hash del serial.

    Un mismo disco cae siempre en el mismo lado, así que el muestreo conserva discos enteros
    (todos sus días). Es el mismo cálculo que `crc32(serial) % 10000` en Spark.
    """
    return zlib.crc32((salt + serial).encode()) % 10_000 < round(pct * 100)


def window_offset_days(first: dt.date, slide_days: int) -> int:
    """Desplazamiento (`startTime` de `window()`) para que una ventana empiece en `first`.

    Spark alinea las ventanas al 1970-01-01; sin desplazamiento, las de 7 días empezarían en
    jueves y la primera ventana de la muestra quedaría partida.
    """
    return (first - EPOCH).days % slide_days


def window_starts(
    first: dt.date, last: dt.date, window_days: int, slide_days: int
) -> list[dt.date]:
    """Inicios de las ventanas (alineadas a `first`) que contienen algún día entre first y last."""
    k_min = -((window_days - 1) // slide_days)
    k_max = (last - first).days // slide_days
    return [first + dt.timedelta(days=k * slide_days) for k in range(k_min, k_max + 1)]


def windows_containing(
    day: dt.date, first: dt.date, window_days: int, slide_days: int
) -> list[dt.date]:
    """Inicios de las ventanas (alineadas a `first`) a las que pertenece `day`."""
    return [
        start
        for start in window_starts(first, day, window_days, slide_days)
        if start <= day < start + dt.timedelta(days=window_days)
    ]


def end_marker_date(last: dt.date, window_days: int, watermark_days: int) -> dt.date:
    """Fecha de la marca de fin: con ella, watermark = marca − umbral ≥ fin de la última ventana.

    La última ventana termina, como muy tarde, `window_days` después del último día.
    """
    return last + dt.timedelta(days=window_days + watermark_days)
