"""Producer de la Parte C: publica en Kafka los registros día por día con reloj acelerado.

Cada «tic» del reloj (por defecto 10 s) es un día de datos: se lee el CSV de ese día, se
filtran los modelos pedidos (unos 120.000 eventos con los 5 modelos por defecto) y se
publican. Un porcentaje de los discos llega con días de retraso, como pasaría con reportes
tardíos: sirve para ver cómo el watermark acepta o descarta los datos desordenados. Al final
se publica una marca de fin de flujo que cierra las últimas ventanas.

Uso:
    python -m part_c.producer --bootstrap kafka:9092 --topic drivestats
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Iterator
from pathlib import Path

from part_c.events import (
    DEFAULT_MODELS,
    LATE_SALT,
    encode,
    end_marker,
    end_marker_date,
    in_bucket,
)

COLUMNS = ("date", "serial_number", "model", "failure")
POLL_EVERY = 1_000

Event = tuple[str, str, str, int]  # (fecha, serial, modelo, falla)


def parse_models(value: str) -> tuple[str, ...]:
    """Lista separada por comas; vacío = modelos por defecto; «all» = sin filtro."""
    value = value.strip()
    if not value:
        return DEFAULT_MODELS
    if value.lower() == "all":
        return ()
    return tuple(m.strip() for m in value.split(",") if m.strip())


def list_days(input_dir: Path) -> list[tuple[dt.date, Path]]:
    """Los CSV diarios de la carpeta, en orden de fecha (el nombre es la fecha)."""
    days = []
    for path in sorted(input_dir.rglob("*.csv")):
        try:
            days.append((dt.date.fromisoformat(path.stem), path))
        except ValueError:
            raise SystemExit(f"{path.name}: el nombre no es una fecha AAAA-MM-DD") from None
    if not days:
        raise SystemExit(f"No hay CSV en {input_dir}")
    return days


def split_row(line: str, width: int) -> list[str]:
    """Las columnas 0…width de una línea CSV.

    Backblaze no usa comillas, así que basta con `split`, que corta solo las primeras columnas
    en vez de parsear las ~190 de cada línea (es 3 veces más rápido). Si una línea trae
    comillas se parsea con el módulo csv.
    """
    if '"' in line:
        return next(csv.reader([line]))
    return line.rstrip("\r\n").split(",", width + 1)


def read_day(path: Path, models: tuple[str, ...], sample_pct: float) -> Iterator[Event]:
    """Filas válidas del CSV con las mismas reglas que la Parte A, filtradas y muestreadas."""
    wanted = set(models)
    with path.open(newline="") as fh:
        header = next(csv.reader([fh.readline()]))
        i_date, i_serial, i_model, i_failure = (header.index(c) for c in COLUMNS)
        width = max(i_date, i_serial, i_model, i_failure)
        for line in fh:
            row = split_row(line, width)
            if len(row) <= width:
                continue
            model, failure = row[i_model].strip(), row[i_failure]
            if not model or failure not in ("0", "1") or (wanted and model not in wanted):
                continue
            serial = row[i_serial]
            if sample_pct < 100 and not in_bucket(serial, sample_pct):
                continue
            yield row[i_date], serial, model, int(failure)


def add_publish_args(parser: argparse.ArgumentParser) -> None:
    """Opciones de publicación; también las usa el job, que lanza el producer con ellas."""
    env = os.environ
    parser.add_argument("--bootstrap", default=env.get("KAFKA_BOOTSTRAP") or "kafka:9092")
    parser.add_argument("--topic", default=env.get("KAFKA_TOPIC") or "drivestats")
    parser.add_argument("--input", type=Path, default=Path("data/raw"), help="carpeta con CSV")
    parser.add_argument(
        "--seconds-per-day",
        type=float,
        default=float(env.get("STREAM_SECONDS_PER_DAY") or 10),
        help="segundos reales por cada día de datos (reloj acelerado)",
    )
    parser.add_argument(
        "--models",
        type=parse_models,
        default=parse_models(env.get("STREAM_MODELS") or ""),
        help="modelos separados por comas; «all» publica todos",
    )
    parser.add_argument(
        "--sample-pct",
        type=float,
        default=float(env.get("STREAM_SAMPLE_PCT") or 100),
        help="porcentaje de discos que se publican (muestreo por hash del serial)",
    )
    parser.add_argument(
        "--late-pct",
        type=float,
        default=float(env.get("STREAM_LATE_PCT") or 2),
        help="porcentaje de discos cuyos eventos llegan con retraso",
    )
    parser.add_argument(
        "--late-days",
        type=int,
        default=int(env.get("STREAM_LATE_DAYS") or 1),
        help="días de retraso de esos eventos",
    )


def check_publish_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.seconds_per_day <= 0:
        parser.error("--seconds-per-day debe ser mayor que 0")
    if not 0 < args.sample_pct <= 100 or not 0 <= args.late_pct <= 100:
        parser.error("--sample-pct debe estar en (0, 100] y --late-pct en [0, 100]")
    if args.late_days < 1 and args.late_pct > 0:
        parser.error("--late-days debe ser 1 o más")


def models_arg(models: tuple[str, ...]) -> str:
    """Inverso de `parse_models`, para pasar los modelos por la línea de comandos."""
    return ",".join(models) if models else "all"


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m part_c.producer",
        description=__doc__.splitlines()[0],
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_publish_args(parser)
    parser.add_argument("--window-days", type=int, default=7, help="para fechar la marca de fin")
    parser.add_argument("--watermark-days", type=int, default=2, help="ídem")
    parser.add_argument("--report", type=Path, help="JSON con lo publicado (lo usa el job)")
    args = parser.parse_args(argv)
    check_publish_args(parser, args)
    return args


class Publisher:
    """Envuelve el Producer de confluent-kafka: reintenta si la cola local se llena."""

    def __init__(self, bootstrap: str, topic: str) -> None:
        from confluent_kafka import Producer  # import diferido: los tests no lo necesitan

        self.topic = topic
        self.errors = 0
        self._pending = 0
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": "drivestats-producer",
                "linger.ms": 50,
                "compression.type": "lz4",
                "queue.buffering.max.messages": 500_000,
            }
        )

    def _delivered(self, err, _msg) -> None:
        if err is not None:
            self.errors += 1

    def send(self, key: str, value: bytes) -> None:
        while True:
            try:
                self.producer.produce(
                    self.topic, value=value, key=key or None, on_delivery=self._delivered
                )
                break
            except BufferError:  # cola local llena: se espera a que salgan mensajes
                self.producer.poll(0.1)
        self._pending += 1
        if self._pending >= POLL_EVERY:  # atender las confirmaciones cada tanto, no por mensaje
            self.producer.poll(0)
            self._pending = 0

    def flush(self) -> None:
        remaining = self.producer.flush(60)
        if remaining:
            raise RuntimeError(f"{remaining} mensajes sin confirmar tras 60 s")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    days = list_days(args.input)
    models = ", ".join(args.models) if args.models else "todos"
    print(
        f"[producer] {len(days)} días ({days[0][0]} … {days[-1][0]}) → topic «{args.topic}» "
        f"en {args.bootstrap} · 1 día cada {args.seconds_per_day:g} s · modelos: {models} · "
        f"muestra {args.sample_pct:g} % · {args.late_pct:g} % de discos con "
        f"{args.late_days} día(s) de retraso",
        flush=True,
    )
    publisher = Publisher(args.bootstrap, args.topic)

    held: dict[int, list[Event]] = defaultdict(list)  # tic → eventos retrasados que tocan
    sent_by_date = Counter()
    late_by_date_model: dict[str, Counter] = defaultdict(Counter)
    total_ticks = len(days) + (args.late_days if args.late_pct > 0 else 0)
    t0 = time.monotonic()

    for tick in range(total_ticks):
        t_tick = time.monotonic()
        late_now = held.pop(tick, [])
        for date, serial, model, failure in late_now:
            publisher.send(serial, encode(date, serial, model, failure))
            sent_by_date[date] += 1
        on_time = 0
        if tick < len(days):
            day, path = days[tick]
            for event in read_day(path, args.models, args.sample_pct):
                date, serial, model, failure = event
                if args.late_pct > 0 and in_bucket(serial, args.late_pct, LATE_SALT):
                    held[tick + args.late_days].append(event)
                    late_by_date_model[date][model] += 1
                    continue
                publisher.send(serial, encode(date, serial, model, failure))
                sent_by_date[date] += 1
                on_time += 1
        publisher.flush()

        label = f"día {days[tick][0]}" if tick < len(days) else "sin datos nuevos"
        late_dates = sorted({e[0] for e in late_now})
        late_text = (
            f" + {len(late_now):,} retrasados de {', '.join(late_dates)}" if late_now else ""
        )
        print(
            f"[producer] tic {tick + 1}/{total_ticks} · {label}: {on_time:,} eventos a tiempo"
            f"{late_text} · {time.monotonic() - t_tick:.1f} s",
            flush=True,
        )
        deadline = t0 + (tick + 1) * args.seconds_per_day
        delay = deadline - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            print(f"[producer] aviso: el tic va {-delay:.1f} s tarde", flush=True)

    marker = end_marker_date(days[-1][0], args.window_days, args.watermark_days)
    publisher.send("", end_marker(marker))
    publisher.flush()
    total = sum(sent_by_date.values())
    print(
        f"[producer] marca de fin con fecha {marker} publicada · {total:,} eventos en total · "
        f"{publisher.errors} errores de entrega",
        flush=True,
    )

    if args.report:
        args.report.write_text(
            json.dumps(
                {
                    "topic": args.topic,
                    "days": [d.isoformat() for d, _ in days],
                    "models": list(args.models),
                    "sample_pct": args.sample_pct,
                    "late_pct": args.late_pct,
                    "late_days": args.late_days,
                    "seconds_per_day": args.seconds_per_day,
                    "events": total,
                    "events_by_date": dict(sorted(sent_by_date.items())),
                    "late_by_date_model": {
                        d: dict(c) for d, c in sorted(late_by_date_model.items())
                    },
                    "end_marker_date": marker.isoformat(),
                    "delivery_errors": publisher.errors,
                },
                indent=2,
                ensure_ascii=False,
            )
        )
    return 0 if publisher.errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
