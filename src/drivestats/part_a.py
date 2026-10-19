"""Parte A: MapReduce sobre Backblaze Drive Stats.

1. Con RDD se emite el par (modelo, (1, falla)) por cada fila y se suma con reduceByKey:
   así salen los días-disco y las fallas por modelo, y con ellos la tasa anualizada (AFR).
2. Se comprueba que el resultado coincide con un groupBy de DataFrames.
3. Se cuentan los días observados por serial, (serial, (1, falla)), lo que introduce la
   censura: en una ventana corta casi todos los discos siguen vivos al final.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from collections import defaultdict
from collections.abc import Iterable, Iterator
from functools import partial
from pathlib import Path

from pyspark import RDD, SparkContext, StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from drivestats.metrics import afr_ci95_pct, afr_pct

COLUMNS = ("date", "serial_number", "model", "failure")
DATE, SERIAL, MODEL, FAILURE = range(len(COLUMNS))

# Cifras del enunciado para el trimestre completo (Backblaze reporta 1,73 %)
BACKBLAZE_Q2_2026 = {"failures": 1498, "drive_days": 31_553_350}

Row = tuple[str, str, str, int]  # (fecha, serial, modelo, falla)


# ---------------------------------------------------------------- lectura ----


def list_inputs(input_dir: Path) -> list[Path]:
    files = sorted(p for p in input_dir.rglob("*.csv") if p.is_file())
    if not files:
        raise SystemExit(
            f"No hay CSV en {input_dir}. Descarga la muestra con: docker compose run --rm download"
        )
    return files


def group_by_header(files: Iterable[Path]) -> dict[tuple[str, ...], list[Path]]:
    """Agrupa los archivos por cabecera: el esquema de Backblaze cambia entre trimestres."""
    groups: dict[tuple[str, ...], list[Path]] = defaultdict(list)
    for path in files:
        with path.open(newline="") as fh:
            header = tuple(next(csv.reader(fh)))
        missing = set(COLUMNS) - set(header)
        if missing:
            raise SystemExit(f"{path.name} no tiene las columnas {sorted(missing)}")
        groups[header].append(path)
    return dict(groups)


def parse_lines(lines: Iterable[str], idx: tuple[int, ...]) -> Iterator[Row]:
    """Convierte líneas CSV en tuplas (fecha, serial, modelo, falla) y descarta cabeceras."""
    i_date, i_serial, i_model, i_failure = idx
    width = max(idx)
    for row in csv.reader(lines):
        if len(row) <= width or row[i_date] == "date":
            continue
        model, failure = row[i_model].strip(), row[i_failure]
        if model and failure in ("0", "1"):
            yield row[i_date], row[i_serial], model, int(failure)


def load_rows(sc: SparkContext, groups: dict[tuple[str, ...], list[Path]]) -> RDD:
    rdds = []
    for header, files in groups.items():
        idx = tuple(header.index(c) for c in COLUMNS)
        lines = sc.textFile(",".join(p.resolve().as_uri() for p in files))
        rdds.append(lines.mapPartitions(partial(parse_lines, idx=idx)))
    return rdds[0] if len(rdds) == 1 else sc.union(rdds)


def load_dataframe(spark: SparkSession, groups: dict[tuple[str, ...], list[Path]]) -> DataFrame:
    """La misma lectura y el mismo filtro que `parse_lines`, pero con DataFrames."""
    frames = [
        spark.read.option("header", True)
        .csv([p.resolve().as_uri() for p in files])
        .select(*COLUMNS)
        for files in groups.values()
    ]
    df = frames[0]
    for other in frames[1:]:
        df = df.unionByName(other)
    df = df.withColumn("model", F.trim("model"))
    return df.where(
        F.col("model").isNotNull() & (F.col("model") != "") & F.col("failure").isin("0", "1")
    ).withColumn("failure", F.col("failure").cast("int"))


# -------------------------------------------------------------- MapReduce ----


def add_pairs(a: tuple[int, int], b: tuple[int, int]) -> tuple[int, int]:
    return a[0] + b[0], a[1] + b[1]


def drive_days_by_model(rows: RDD, num_partitions: int | None = None) -> RDD:
    """(modelo, (1, falla)) → reduceByKey → (modelo, (días-disco, fallas))."""
    pairs = rows.map(lambda r: (r[MODEL], (1, r[FAILURE])))
    return pairs.reduceByKey(add_pairs, num_partitions)


def drive_days_by_model_df(df: DataFrame) -> DataFrame:
    return df.groupBy("model").agg(
        F.count("*").alias("drive_days"), F.sum("failure").alias("failures")
    )


def merge_serial(a: tuple, b: tuple) -> tuple:
    # (días observados, fallas, modelo, primer día, último día)
    return a[0] + b[0], a[1] + b[1], a[2], min(a[3], b[3]), max(a[4], b[4])


def days_by_serial(rows: RDD, num_partitions: int | None = None) -> RDD:
    """(serial, (1, falla, …)) → reduceByKey → días observados y fallas de cada disco."""
    pairs = rows.map(lambda r: (r[SERIAL], (1, r[FAILURE], r[MODEL], r[DATE], r[DATE])))
    return pairs.reduceByKey(merge_serial, num_partitions)


def censoring_by_model(serials: RDD, num_partitions: int | None = None) -> RDD:
    """(modelo, (discos, discos que fallaron, días observados)) a partir de cada serial."""
    pairs = serials.map(lambda kv: (kv[1][2], (1, min(kv[1][1], 1), kv[1][0])))
    return pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1], a[2] + b[2]), num_partitions)


def days_observed_histogram(serials: RDD) -> dict[int, tuple[int, int]]:
    """Días observados → (discos, discos que fallaron)."""
    pairs = serials.map(lambda kv: (kv[1][0], (1, min(kv[1][1], 1))))
    return dict(pairs.reduceByKey(add_pairs).collect())


# ---------------------------------------------------------------- salida ----


def _cell(value) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


def print_table(headers: list[str], rows: list[list], title: str) -> None:
    cells = [[_cell(v) for v in row] for row in rows]
    widths = [max([len(h)] + [len(r[i]) for r in cells]) for i, h in enumerate(headers)]
    # Texto a la izquierda; números y porcentajes a la derecha
    left = [
        bool(rows) and isinstance(rows[0][i], str) and not rows[0][i].endswith("%")
        for i in range(len(headers))
    ]

    def fmt(values: list[str]) -> str:
        return "  ".join(
            v.ljust(w) if lft else v.rjust(w)
            for v, w, lft in zip(values, widths, left, strict=True)
        )

    print(f"\n{title}")
    print(fmt(headers))
    print("  ".join("-" * w for w in widths))
    for row in cells:
        print(fmt(row))


def write_csv(path: Path, headers: list[str], rows: Iterable[Iterable]) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        writer.writerows(rows)


def afr_row(model: str, drive_days: int, failures: int) -> list:
    low, high = afr_ci95_pct(failures, drive_days)
    return [
        model,
        drive_days,
        round(drive_days / 365, 1),
        failures,
        round(afr_pct(failures, drive_days), 2),
        round(low, 2),
        round(high, 2),
    ]


# ------------------------------------------------------------------ main ----


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Parte A: MapReduce de días-disco y fallas")
    parser.add_argument("--input", type=Path, default=Path("data/raw"), help="carpeta con CSV")
    parser.add_argument("--output", type=Path, default=Path("output/part_a"))
    parser.add_argument(
        "--partitions", type=int, help="particiones de los reduceByKey y del groupBy"
    )
    parser.add_argument(
        "--min-drive-days",
        type=int,
        default=10_000,
        help="días-disco mínimos para mostrar un modelo en la tabla de AFR",
    )
    parser.add_argument("--top", type=int, default=25, help="filas a mostrar por tabla")
    args = parser.parse_args(argv)

    builder = SparkSession.builder.appName("parte-a-mapreduce")
    if args.partitions:
        builder = builder.config("spark.sql.shuffle.partitions", args.partitions)
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    files = list_inputs(args.input)
    groups = group_by_header(files)
    args.output.mkdir(parents=True, exist_ok=True)
    timings: dict[str, float] = {}

    rows = load_rows(sc, groups).persist(StorageLevel.MEMORY_AND_DISK)
    print(
        f"Spark {spark.version} en {sc.master} · {len(files)} archivos en {args.input} · "
        f"{rows.getNumPartitions()} particiones de entrada"
    )

    # 1) MapReduce con RDD: días-disco y fallas por modelo --------------------
    t0 = time.perf_counter()
    by_model = dict(drive_days_by_model(rows, args.partitions).collect())
    timings["rdd_reduce_by_model"] = time.perf_counter() - t0

    dates = sorted(rows.map(lambda r: r[DATE]).distinct().collect())
    total_days = sum(dd for dd, _ in by_model.values())
    total_failures = sum(f for _, f in by_model.values())
    print(
        f"Ventana: {dates[0]} a {dates[-1]} ({len(dates)} días) · {total_days:,} filas "
        f"(= días-disco) · {total_failures:,} fallas · {len(by_model)} modelos"
    )

    afr_headers = [
        "model",
        "drive_days",
        "drive_years",
        "failures",
        "afr_pct",
        "afr_ci95_low",
        "afr_ci95_high",
    ]
    afr_rows = [
        afr_row(m, dd, f)
        for m, (dd, f) in sorted(by_model.items(), key=lambda kv: (-kv[1][0], kv[0]))
    ]
    write_csv(args.output / "afr_por_modelo.csv", afr_headers, afr_rows)
    shown = [r for r in afr_rows if r[1] >= args.min_drive_days]
    print_table(
        afr_headers,
        shown[: args.top],
        f"AFR por modelo (RDD + reduceByKey), modelos con ≥ {args.min_drive_days:,} días-disco "
        f"({len(shown)} de {len(afr_rows)}; se muestran {min(args.top, len(shown))}):",
    )
    small = len(afr_rows) - len(shown)
    print(
        f"{small} modelos tienen menos de {args.min_drive_days:,} días-disco: su AFR sale de "
        "muy pocos datos y no es interpretable (ver afr_por_modelo.csv)."
    )

    global_afr = afr_row("TOTAL", total_days, total_failures)
    reference = afr_pct(BACKBLAZE_Q2_2026["failures"], BACKBLAZE_Q2_2026["drive_days"])
    print(
        f"\nAFR global de la muestra: {global_afr[4]:.2f} % "
        f"(IC 95 %: {global_afr[5]:.2f} – {global_afr[6]:.2f} %). "
        f"Misma fórmula sobre el trimestre completo que publica Backblaze: {reference:.2f} %."
    )

    # 2) Verificación contra un groupBy de DataFrames --------------------------
    t0 = time.perf_counter()
    df = load_dataframe(spark, groups)
    by_model_df = {
        r["model"]: (r["drive_days"], r["failures"]) for r in drive_days_by_model_df(df).collect()
    }
    timings["dataframe_group_by"] = time.perf_counter() - t0

    differences = sorted(
        m for m in by_model.keys() | by_model_df.keys() if by_model.get(m) != by_model_df.get(m)
    )
    if differences:
        print(f"\n✗ RDD y DataFrame difieren en {len(differences)} modelos:")
        for m in differences[: args.top]:
            print(f"  {m}: RDD={by_model.get(m)}  DataFrame={by_model_df.get(m)}")
    else:
        print(
            f"\n✓ RDD (reduceByKey) y DataFrame (groupBy) coinciden en los {len(by_model)} "
            "modelos: mismos días-disco y mismas fallas."
        )

    # 3) Días observados por serial: censura ------------------------------------
    t0 = time.perf_counter()
    serials = days_by_serial(rows, args.partitions).persist(StorageLevel.MEMORY_AND_DISK)
    serial_headers = [
        "serial_number",
        "model",
        "first_seen",
        "last_seen",
        "days_observed",
        "failed",
        "censored",
    ]
    n_drives = n_failed = days_sum = multi_failure = overlong = 0
    with (args.output / "vida_por_serial.csv").open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(serial_headers)
        for serial, (days, failures, model, first, last) in serials.toLocalIterator():
            failed = min(failures, 1)
            writer.writerow([serial, model, first, last, days, failed, 1 - failed])
            n_drives += 1
            n_failed += failed
            days_sum += days
            multi_failure += failures > 1
            overlong += days > len(dates)
    histogram = days_observed_histogram(serials)
    censoring = sorted(
        censoring_by_model(serials, args.partitions).collect(), key=lambda kv: -kv[1][0]
    )
    timings["rdd_reduce_by_serial"] = time.perf_counter() - t0

    n_censored = n_drives - n_failed
    print(
        f"\nDías observados por serial: {n_drives:,} discos; {n_failed:,} fallaron en la "
        f"ventana y {n_censored:,} ({n_censored / n_drives:.2%}) siguen vivos al final "
        "(censurados por la derecha: solo se sabe que aún no han fallado)."
    )
    print_table(
        ["days_observed", "drives", "pct_drives", "failed"],
        [[d, n, f"{n / n_drives:.2%}", f] for d, (n, f) in sorted(histogram.items(), reverse=True)],
        "Distribución de días observados:",
    )

    censoring_headers = [
        "model",
        "drives",
        "failed_drives",
        "censored_drives",
        "pct_censored",
        "mean_days_observed",
    ]
    censoring_rows = [
        [m, n, f, n - f, round(100 * (n - f) / n, 2), round(d / n, 2)] for m, (n, f, d) in censoring
    ]
    write_csv(args.output / "censura_por_modelo.csv", censoring_headers, censoring_rows)
    print_table(
        censoring_headers,
        censoring_rows[: args.top],
        f"Censura por modelo (se muestran {min(args.top, len(censoring_rows))} "
        f"de {len(censoring_rows)}):",
    )

    consistent = days_sum == total_days
    print(
        f"\nControles: Σ días observados = {days_sum:,} "
        f"{'=' if consistent else '≠'} días-disco ({total_days:,}); "
        f"seriales con más de una falla: {multi_failure}; "
        f"seriales con más días que la ventana: {overlong}."
    )

    summary = {
        "input": {
            "files": [p.name for p in files],
            "first_date": dates[0],
            "last_date": dates[-1],
            "days": len(dates),
            "rows": total_days,
        },
        "spark": {
            "version": spark.version,
            "master": sc.master,
            "input_partitions": rows.getNumPartitions(),
            "reduce_partitions": args.partitions,
        },
        "afr": {
            "models": len(by_model),
            "drive_days": total_days,
            "failures": total_failures,
            "afr_pct": global_afr[4],
            "afr_ci95_pct": [global_afr[5], global_afr[6]],
            "backblaze_q2_2026_reference_pct": round(reference, 2),
        },
        "rdd_vs_dataframe": {
            "models_compared": len(by_model.keys() | by_model_df.keys()),
            "differences": differences,
            "match": not differences,
        },
        "censoring": {
            "drives": n_drives,
            "failed": n_failed,
            "censored": n_censored,
            "pct_censored": round(100 * n_censored / n_drives, 2),
            "days_observed_histogram": {
                str(d): {"drives": n, "failed": f} for d, (n, f) in sorted(histogram.items())
            },
            "sum_days_equals_drive_days": consistent,
            "serials_with_multiple_failures": multi_failure,
            "serials_with_more_days_than_window": overlong,
        },
        "timings_s": {k: round(v, 3) for k, v in timings.items()},
    }
    (args.output / "resumen.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(
        f"\nResultados en {args.output}/: afr_por_modelo.csv, censura_por_modelo.csv, "
        "vida_por_serial.csv, resumen.json"
    )

    spark.stop()
    return 0 if not differences and consistent else 1
