"""Parte C: Structured Streaming sobre Kafka, por modelo, en ventanas de 7 días.

1. El producer publica en Kafka un día de datos cada 10 s (lo lanza este mismo job).
2. Spark lee el topic y agrupa por modelo en ventanas de 7 días de *tiempo de evento* (la
   fecha de cada fila), con un watermark de 2 días. Calcula días-disco, fallas y AFR.
3. Dos consultas sobre la misma agregación:
   - «en_vivo» (modo update): en cada lote muestra las ventanas que cambiaron.
   - «ventanas_cerradas» (modo append): emite una ventana solo cuando el watermark pasa su
     final, es decir, cuando Spark ya no aceptará más datos para ella. Es el resultado final.
4. Cuando se cierran todas las ventanas, compara el resultado con el mismo cálculo en batch
   sobre los CSV: deben coincidir, salvo los eventos tardíos que el watermark descarte.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from drivestats.metrics import afr_ci95_pct, afr_pct
from part_a.mapreduce import group_by_header, list_inputs, load_dataframe, print_table
from part_c.events import (
    END_MODEL,
    EVENT_SCHEMA,
    window_offset_days,
    window_starts,
    windows_containing,
)
from part_c.producer import add_publish_args, check_publish_args, list_days, models_arg

LIVE, FINAL = "en_vivo", "ventanas_cerradas"
WINDOW_HEADERS = [
    "window_start",
    "window_end",
    "model",
    "drive_days",
    "failures",
    "afr_pct",
    "afr_ci95_low",
    "afr_ci95_high",
]
GENERATED = (
    "ventanas.csv",
    "actualizaciones.csv",
    "progreso.csv",
    "verificacion.csv",
    "productor.json",
    "resumen.json",
)

# Desglose del tiempo de cada lote (StreamingQueryProgress.durationMs)
DURATIONS = (
    "triggerExecution",
    "latestOffset",
    "queryPlanning",
    "addBatch",
    "walCommit",
    "commitOffsets",
)

WindowKey = tuple[str, str]  # (inicio de la ventana, modelo)
Counts = dict[WindowKey, tuple[int, int]]  # → (días-disco, fallas)


# --------------------------------------------------------------- Kafka ----


def reset_topic(bootstrap: str, topic: str, partitions: int, timeout_s: float = 60) -> None:
    """Borra y vuelve a crear el topic: cada ejecución empieza con el topic vacío."""
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        for future in admin.delete_topics([topic], operation_timeout=30).values():
            future.result()
    deadline = time.monotonic() + timeout_s
    while True:
        new = NewTopic(topic, num_partitions=partitions, replication_factor=1)
        try:
            for future in admin.create_topics([new]).values():
                future.result()
            return
        except KafkaException:
            # El borrado es asíncrono: hasta que termina, crear falla con «ya existe»
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


# ------------------------------------------------------------ streaming ----


def read_events(spark: SparkSession, bootstrap: str, topic: str, watermark_days: int) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    events = raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
    return (
        events.select("e.*")
        .withColumn("event_time", F.to_timestamp("date", "yyyy-MM-dd"))
        .where(F.col("event_time").isNotNull())
        .withWatermark("event_time", f"{watermark_days} days")
    )


def window_column(window_days: int, slide_days: int, offset_days: int) -> Column:
    return F.window(
        "event_time", f"{window_days} days", f"{slide_days} days", f"{offset_days} days"
    )


def windowed_counts(events: DataFrame, window: Column) -> DataFrame:
    """La misma agregación vale para el stream y para el cálculo batch de verificación.

    La marca de fin (END_MODEL) no se filtra aquí: Spark empujaría el filtro por debajo del
    watermark y la marca ya no lo haría avanzar. Se descarta al leer los resultados.
    """
    return events.groupBy(window.alias("window"), "model").agg(
        F.count("*").alias("drive_days"), F.sum("failure").alias("failures")
    )


def collect_windows(df: DataFrame) -> list[tuple[str, str, str, int, int]]:
    rows = (
        df.where(F.col("model") != END_MODEL)
        .select(
            F.date_format("window.start", "yyyy-MM-dd").alias("start"),
            F.date_format("window.end", "yyyy-MM-dd").alias("end"),
            "model",
            "drive_days",
            "failures",
        )
        .orderBy("start", "model")
        .collect()
    )
    return [(r["start"], r["end"], r["model"], r["drive_days"], r["failures"]) for r in rows]


def span(start: str, end: str) -> str:
    """Ventana [inicio, fin) como «inicio … último día»."""
    last = dt.date.fromisoformat(end) - dt.timedelta(days=1)
    return f"{start} … {last.isoformat()}"


def window_row(start: str, end: str, model: str, drive_days: int, failures: int) -> list:
    low, high = afr_ci95_pct(failures, drive_days)
    afr = afr_pct(failures, drive_days)
    return [start, end, model, drive_days, failures, round(afr, 2), round(low, 2), round(high, 2)]


def watermark_date(progress_watermark: str | None) -> str:
    if not progress_watermark or progress_watermark.startswith("1970-01-01"):
        return "—"
    return progress_watermark[:10]


class Run:
    """Estado compartido entre los hilos de las consultas, el listener y el producer."""

    def __init__(self, output: Path) -> None:
        self.output = output
        self.lock = threading.Lock()
        self.closed: Counts = {}
        self.closed_windows: set[str] = set()
        self.progress: list[dict] = []
        self.events_read = Counter()
        self.dropped = Counter()
        self._last_watermark = ""
        for name, headers in (
            ("ventanas.csv", WINDOW_HEADERS + ["batch_id"]),
            ("actualizaciones.csv", ["batch_id"] + WINDOW_HEADERS[:6]),
        ):
            with (output / name).open("w", newline="") as fh:
                csv.writer(fh).writerow(headers)

    def say(self, *lines: str) -> None:
        with self.lock:
            for line in lines:
                print(line, flush=True)

    def append_csv(self, name: str, rows: list[list]) -> None:
        with (self.output / name).open("a", newline="") as fh:
            csv.writer(fh).writerows(rows)

    # foreachBatch de «en_vivo» (update): ventanas que cambiaron en el lote
    def on_live_batch(self, df: DataFrame, batch_id: int) -> None:
        rows = [window_row(*w) for w in collect_windows(df)]
        if not rows:
            return
        with self.lock:
            self.append_csv("actualizaciones.csv", [[batch_id] + r[:6] for r in rows])
            print_table(
                ["window", "model", "drive_days", "failures", "afr_pct"],
                [[span(r[0], r[1]), *r[2:6]] for r in rows],
                f"[{LIVE}] lote {batch_id} · ventanas actualizadas (modo update, aún abiertas):",
            )

    # foreachBatch de «ventanas_cerradas» (append): solo ventanas que el watermark ya cerró
    def on_final_batch(self, df: DataFrame, batch_id: int) -> None:
        rows = [window_row(*w) for w in collect_windows(df)]
        if not rows:
            return
        with self.lock:
            self.append_csv("ventanas.csv", [r + [batch_id] for r in rows])
            for r in rows:
                self.closed[(r[0], r[2])] = (r[3], r[4])
                self.closed_windows.add(r[0])
            print_table(
                ["window", "model", "drive_days", "failures", "afr_pct", "afr_ci95"],
                [[span(r[0], r[1]), *r[2:6], f"{r[6]:.2f} – {r[7]:.2f}"] for r in rows],
                f"[{FINAL}] lote {batch_id} · el watermark pasó el final de la ventana: "
                "resultado definitivo (modo append):",
            )

    def on_progress(self, p) -> None:
        dropped = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        state_rows = sum(op.numRowsTotal for op in p.stateOperators)
        watermark = (p.eventTime or {}).get("watermark")
        with self.lock:
            self.events_read[p.name] += p.numInputRows
            self.dropped[p.name] += dropped
            self.progress.append(
                {
                    "query": p.name,
                    "batch_id": p.batchId,
                    "input_rows": p.numInputRows,
                    "watermark": watermark_date(watermark),
                    "rows_dropped_by_watermark": dropped,
                    "state_rows": state_rows,
                    **{f"{k}_ms": (p.durationMs or {}).get(k, 0) for k in DURATIONS},
                }
            )
            if p.name == LIVE and (p.numInputRows or watermark != self._last_watermark):
                self._last_watermark = watermark
                seconds = (p.durationMs or {}).get("triggerExecution", 0) / 1000
                print(
                    f"  ↳ [{LIVE}] lote {p.batchId}: {p.numInputRows:,} "
                    f"evento{'' if p.numInputRows == 1 else 's'} de Kafka en "
                    f"{seconds:.1f} s · watermark {watermark_date(watermark)} · filas en el "
                    f"estado {state_rows:,} · descartadas por el watermark {dropped:,}",
                    flush=True,
                )


class ProgressListener(StreamingQueryListener):
    def __init__(self, run: Run) -> None:
        super().__init__()
        self.run = run

    def onQueryStarted(self, event) -> None:
        pass

    def onQueryProgress(self, event) -> None:
        self.run.on_progress(event.progress)

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        pass


# --------------------------------------------------------- verificación ----


def batch_counts(
    spark: SparkSession,
    groups: dict,
    models: tuple[str, ...],
    sample_pct: float,
    window: Column,
) -> Counts:
    """El mismo cálculo en batch sobre los CSV, con el mismo filtro y muestreo del producer."""
    df = load_dataframe(spark, groups)
    if models:
        df = df.where(F.col("model").isin(list(models)))
    if sample_pct < 100:
        bucket = F.crc32(F.col("serial_number").cast("binary")) % 10_000
        df = df.where(bucket < round(sample_pct * 100))
    df = df.withColumn("event_time", F.to_timestamp("date", "yyyy-MM-dd"))
    return {(s, m): (dd, f) for s, _, m, dd, f in collect_windows(windowed_counts(df, window))}


def late_by_window(
    report: dict, first: dt.date, window_days: int, slide_days: int
) -> dict[WindowKey, int]:
    """Eventos que el producer publicó con retraso, por (ventana, modelo)."""
    late: Counter = Counter()
    for date, by_model in report.get("late_by_date_model", {}).items():
        day = dt.date.fromisoformat(date)
        for start in windows_containing(day, first, window_days, slide_days):
            for model, n in by_model.items():
                late[(start.isoformat(), model)] += n
    return dict(late)


def compare(stream: Counts, batch: Counts, late: dict[WindowKey, int]) -> list[dict]:
    """Ventana a ventana: «igual», «tardíos descartados» (explicable) o «error»."""
    rows = []
    for key in sorted(stream.keys() | batch.keys()):
        (s_days, s_fail), (b_days, b_fail) = stream.get(key, (0, 0)), batch.get(key, (0, 0))
        missing, late_events = b_days - s_days, late.get(key, 0)
        if (s_days, s_fail) == (b_days, b_fail):
            status = "igual"
        elif 0 < missing <= late_events and 0 <= b_fail - s_fail <= missing:
            status = "tardíos descartados"
        else:
            status = "error"
        rows.append(
            {
                "window_start": key[0],
                "model": key[1],
                "stream_drive_days": s_days,
                "batch_drive_days": b_days,
                "stream_failures": s_fail,
                "batch_failures": b_fail,
                "late_events": late_events,
                "status": status,
            }
        )
    return rows


# ------------------------------------------------------------------ main ----


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parte C: Structured Streaming sobre Kafka",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_publish_args(parser)
    parser.add_argument("--output", type=Path, default=Path("output/part_c"))
    parser.add_argument("--window-days", type=int, default=7, help="duración de la ventana")
    parser.add_argument(
        "--slide-days", type=int, help="desplazamiento; por defecto = ventana (no se solapan)"
    )
    parser.add_argument("--watermark-days", type=int, default=2, help="umbral del watermark")
    parser.add_argument(
        "--trigger-seconds", type=float, help="un lote cada N s; por defecto, un día simulado"
    )
    parser.add_argument("--topic-partitions", type=int, default=4)
    parser.add_argument(
        "--state-partitions", type=int, default=4, help="particiones del estado (shuffle)"
    )
    parser.add_argument(
        "--no-producer",
        action="store_true",
        help="no lanzar el producer: publícalo desde otro terminal (servicio part-c-producer)",
    )
    parser.add_argument("--timeout", type=float, help="segundos máximos; por defecto, según días")
    args = parser.parse_args(argv)
    check_publish_args(parser, args)
    args.slide_days = args.slide_days or args.window_days
    args.trigger_seconds = args.trigger_seconds or args.seconds_per_day
    if not 1 <= args.slide_days <= args.window_days or args.watermark_days < 0:
        parser.error("se necesita 1 ≤ --slide-days ≤ --window-days y --watermark-days ≥ 0")
    return args


def start_producer(args: argparse.Namespace, run: Run) -> subprocess.Popen:
    cmd = [
        sys.executable,
        "-m",
        "part_c.producer",
        "--bootstrap",
        args.bootstrap,
        "--topic",
        args.topic,
        "--input",
        str(args.input),
        "--seconds-per-day",
        str(args.seconds_per_day),
        "--models",
        models_arg(args.models),
        "--sample-pct",
        str(args.sample_pct),
        "--late-pct",
        str(args.late_pct),
        "--late-days",
        str(args.late_days),
        "--window-days",
        str(args.window_days),
        "--watermark-days",
        str(args.watermark_days),
        "--report",
        str(args.output / "productor.json"),
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    def relay() -> None:  # la salida del producer pasa por el mismo cerrojo que las tablas
        assert proc.stdout is not None
        for line in proc.stdout:
            run.say(line.rstrip())

    threading.Thread(target=relay, daemon=True).start()
    return proc


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    days = list_days(args.input)
    first, last = days[0][0], days[-1][0]
    offset = window_offset_days(first, args.slide_days)
    expected = {
        s.isoformat() for s in window_starts(first, last, args.window_days, args.slide_days)
    }
    late_ticks = args.late_days if args.late_pct > 0 else 0
    timeout = args.timeout or (len(days) + late_ticks) * args.seconds_per_day + 180

    args.output.mkdir(parents=True, exist_ok=True)
    checkpoints = args.output / "checkpoints"
    shutil.rmtree(checkpoints, ignore_errors=True)
    for name in GENERATED:
        (args.output / name).unlink(missing_ok=True)
    reset_topic(args.bootstrap, args.topic, args.topic_partitions)

    spark = (
        SparkSession.builder.appName("parte-c-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", args.state_partitions)
        .config("spark.sql.adaptive.enabled", "false")  # el streaming con estado no lo usa
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run = Run(args.output)
    spark.streams.addListener(ProgressListener(run))

    window = window_column(args.window_days, args.slide_days, offset)
    counts = windowed_counts(
        read_events(spark, args.bootstrap, args.topic, args.watermark_days), window
    )
    trigger = {"processingTime": f"{args.trigger_seconds:g} seconds"}
    final_q = (
        counts.writeStream.queryName(FINAL)
        .outputMode("append")
        .foreachBatch(run.on_final_batch)
        .option("checkpointLocation", str(checkpoints / FINAL))
        .trigger(**trigger)
        .start()
    )
    live_q = (
        counts.writeStream.queryName(LIVE)
        .outputMode("update")
        .foreachBatch(run.on_live_batch)
        .option("checkpointLocation", str(checkpoints / LIVE))
        .trigger(**trigger)
        .start()
    )
    kind = (
        "no se solapan" if args.slide_days == args.window_days else f"cada {args.slide_days} días"
    )
    run.say(
        f"Spark {spark.version} en {spark.sparkContext.master} · Kafka {args.bootstrap}, "
        f"topic «{args.topic}» ({args.topic_partitions} particiones, vaciado al empezar)",
        f"Ventanas de {args.window_days} días ({kind}) desde el {first} · watermark "
        f"{args.watermark_days} días · un lote cada {args.trigger_seconds:g} s · "
        f"se esperan {len(expected)} ventana(s): {', '.join(sorted(expected))}",
    )

    producer = None if args.no_producer else start_producer(args, run)
    t0 = time.monotonic()
    failure = None
    try:
        while True:
            with run.lock:
                if expected <= run.closed_windows:
                    break
            for query in (final_q, live_q):
                if query.exception() is not None:
                    failure = f"la consulta «{query.name}» falló: {query.exception()}"
            if producer is not None and producer.poll() not in (None, 0):
                failure = f"el producer terminó con código {producer.returncode}"
            if time.monotonic() - t0 > timeout:
                failure = f"tiempo agotado ({timeout:.0f} s) sin cerrar todas las ventanas"
            if failure:
                break
            time.sleep(0.5)
        if not failure:
            live_q.processAllAvailable()
    except KeyboardInterrupt:
        failure = "interrumpido"
    finally:
        for query in (live_q, final_q):
            query.stop()
        if producer is not None:
            if failure and producer.poll() is None:
                producer.terminate()
            try:
                producer.wait(timeout=30)
            except subprocess.TimeoutExpired:
                producer.kill()

    with (args.output / "progreso.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(run.progress[0]) if run.progress else ["query"])
        writer.writeheader()
        writer.writerows(run.progress)
    if failure:
        run.say(f"\n✗ {failure}")
        spark.stop()
        return 1

    # Verificación: streaming frente al mismo cálculo en batch sobre los CSV
    report_path = args.output / "productor.json"
    report = json.loads(report_path.read_text()) if report_path.exists() else {}
    models = tuple(report.get("models", args.models))
    sample_pct = report.get("sample_pct", args.sample_pct)
    groups = group_by_header(list_inputs(args.input))
    batch = batch_counts(spark, groups, models, sample_pct, window)
    late = late_by_window(report, first, args.window_days, args.slide_days)
    checks = compare(run.closed, batch, late)
    if not checks:
        run.say("\n✗ No hay ventanas que verificar: ¿los modelos pedidos están en los datos?")
        spark.stop()
        return 1
    with (args.output / "verificacion.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(checks[0]))
        writer.writeheader()
        writer.writerows(checks)
    print_table(
        [
            "window",
            "model",
            "stream_days",
            "batch_days",
            "stream_fail",
            "batch_fail",
            "late",
            "status",
        ],
        [
            [
                c["window_start"],
                c["model"],
                c["stream_drive_days"],
                c["batch_drive_days"],
                c["stream_failures"],
                c["batch_failures"],
                c["late_events"],
                c["status"],
            ]
            for c in checks
        ],
        "Verificación: ventanas cerradas por streaming frente al mismo cálculo en batch:",
    )
    statuses = Counter(c["status"] for c in checks)
    published = report.get("events")
    published_text = f"{published:,}" if published is not None else "?"
    read = run.events_read[FINAL]
    if statuses["error"]:
        verdict = f"✗ {statuses['error']} pares (ventana, modelo) no cuadran con el batch"
    elif statuses["tardíos descartados"]:
        lost = sum(c["batch_drive_days"] - c["stream_drive_days"] for c in checks)
        verdict = (
            f"✓ {statuses['igual']} pares iguales; en {statuses['tardíos descartados']} faltan "
            f"{lost:,} eventos, todos tardíos: el watermark los descartó porque su ventana ya "
            "estaba cerrada"
        )
    else:
        verdict = (
            f"✓ streaming = batch en los {len(checks)} pares (ventana, modelo), incluidos los "
            "eventos que llegaron tarde dentro del watermark"
        )
    run.say(
        "",
        verdict,
        f"Eventos: {published_text} publicados por el producer "
        f"(+1 marca de fin) · {read:,} leídos de Kafka por «{FINAL}» · "
        f"{run.dropped[FINAL]:,} filas descartadas por el watermark en el estado",
    )

    summary = {
        "input": {"days": [d.isoformat() for d, _ in days]},
        "spark": {"version": spark.version, "master": spark.sparkContext.master},
        "stream": {
            "topic": args.topic,
            "topic_partitions": args.topic_partitions,
            "window_days": args.window_days,
            "slide_days": args.slide_days,
            "watermark_days": args.watermark_days,
            "trigger_seconds": args.trigger_seconds,
            "window_offset_days": offset,
            "expected_windows": sorted(expected),
        },
        "producer": report,
        "events_read": dict(run.events_read),
        "rows_dropped_by_watermark": dict(run.dropped),
        "verification": {"statuses": dict(statuses), "ok": not statuses["error"]},
        "elapsed_s": round(time.monotonic() - t0, 1),
    }
    (args.output / "resumen.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    run.say(
        f"\nResultados en {args.output}/: ventanas.csv, actualizaciones.csv, progreso.csv, "
        "verificacion.csv, productor.json, resumen.json"
    )
    spark.stop()
    return 0 if not statuses["error"] else 1
