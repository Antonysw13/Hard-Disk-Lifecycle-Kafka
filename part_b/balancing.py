"""Parte B: balanceo de carga sobre la MapReduce de la Parte A.

1. Tiempos. Se repite la MapReduce de la Parte A (días-disco y fallas por modelo, y días
   observados por serial) con 2, 8, 32 y 200 particiones, varias veces por cada valor.
2. Sesgo. Se reparten las filas por modelo (hash de la clave) y sin clave (round-robin) y se
   cuentan las filas de cada partición con spark_partition_id().

La explicación de la lógica y de los resultados está en part_b/README.md.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path

from pyspark import RDD, StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from part_a.mapreduce import (
    days_by_serial,
    drive_days_by_model,
    group_by_header,
    list_inputs,
    load_dataframe,
    load_rows,
    print_table,
    write_csv,
)

DEFAULT_PARTITIONS = (2, 8, 32, 200)
BY_MODEL, NO_KEY = "por_modelo", "sin_clave"
STRATEGY_TEXT = {
    BY_MODEL: "repartition(n, 'model'): hash del modelo",
    NO_KEY: "repartition(n): round-robin, sin clave",
}

# ------------------------------------------------- estadísticas (Python puro) ----


def time_stats(seconds: Sequence[float]) -> dict[str, float]:
    return {
        "median_s": statistics.median(seconds),
        "min_s": min(seconds),
        "max_s": max(seconds),
        "stdev_s": statistics.stdev(seconds) if len(seconds) > 1 else 0.0,
    }


def partition_stats(counts: Sequence[int]) -> dict[str, float]:
    """Filas por partición → métricas de balance. max/media = 1 es un reparto perfecto."""
    mean = sum(counts) / len(counts)
    return {
        "partitions": len(counts),
        "empty": sum(c == 0 for c in counts),
        "min_rows": min(counts),
        "max_rows": max(counts),
        "mean_rows": mean,
        "max_over_mean": max(counts) / mean if mean else 0.0,
        "cv": statistics.pstdev(counts) / mean if mean else 0.0,
    }


def key_skew_floor(largest_key_rows: int, total_rows: int, partitions: int) -> float:
    """Mínimo max/media posible al repartir por clave: las filas de una clave no se separan,
    así que la partición más grande tiene al menos las de la clave más grande."""
    return max(1.0, largest_key_rows / (total_rows / partitions))


def concentration(rows_by_key: dict[str, int], tops: Sequence[int] = (1, 3, 5, 10)) -> dict:
    """Qué parte de las filas se llevan las claves más grandes."""
    sizes = sorted(rows_by_key.values(), reverse=True)
    total = sum(sizes)
    keys_for: dict[int, int] = {}
    cumulative = 0
    for i, size in enumerate(sizes, start=1):
        cumulative += size
        for pct in (50, 80, 90):
            if pct not in keys_for and cumulative * 100 >= pct * total:
                keys_for[pct] = i
    return {
        "keys": len(sizes),
        "top_share_pct": {k: 100 * sum(sizes[:k]) / total for k in tops if k <= len(sizes)},
        "keys_for_pct": keys_for,
    }


# ---------------------------------------------------------------- tiempos ----


def run_part_a(rows: RDD, partitions: int) -> tuple[float, float]:
    """Las dos MapReduce de la Parte A con `partitions` particiones de reduce."""
    t0 = time.perf_counter()
    drive_days_by_model(rows, partitions).collect()
    t1 = time.perf_counter()
    days_by_serial(rows, partitions).count()
    return t1 - t0, time.perf_counter() - t1


def measure_times(rows: RDD, partitions: Sequence[int], repeats: int) -> list[dict]:
    runs = []
    for n in partitions:
        # Entrada y reduce con n particiones. El reparto se materializa antes de medir para
        # cronometrar solo la MapReduce, no la lectura de los CSV ni el propio repartition.
        data = rows.repartition(n).persist(StorageLevel.MEMORY_AND_DISK)
        data.count()
        run_part_a(data, n)  # calentamiento: arranca los workers de Python de cada executor
        for rep in range(1, repeats + 1):
            by_model_s, by_serial_s = run_part_a(data, n)
            runs.append(
                {
                    "partitions": n,
                    "repetition": rep,
                    "by_model_s": by_model_s,
                    "by_serial_s": by_serial_s,
                    "total_s": by_model_s + by_serial_s,
                }
            )
            print(
                f"  {n:>3} particiones · repetición {rep}/{repeats}: "
                f"{by_model_s + by_serial_s:.2f} s",
                flush=True,
            )
        data.unpersist()
    return runs


def summarize_times(runs: list[dict], partitions: Sequence[int]) -> list[dict]:
    summary = []
    for n in partitions:
        mine = [r for r in runs if r["partitions"] == n]
        summary.append(
            {
                "partitions": n,
                "runs": len(mine),
                **time_stats([r["total_s"] for r in mine]),
                "median_by_model_s": statistics.median(r["by_model_s"] for r in mine),
                "median_by_serial_s": statistics.median(r["by_serial_s"] for r in mine),
            }
        )
    baseline = summary[0]["median_s"]
    for row in summary:
        row["speedup"] = baseline / row["median_s"]
    return summary


# ------------------------------------------------------------------ sesgo ----


def rows_per_partition(
    df: DataFrame, partitions: int, strategy: str
) -> tuple[list[int], dict[int, list[tuple[str, int]]]]:
    """Filas por partición tras el reparto; con clave, también los modelos de cada una."""
    if strategy == BY_MODEL:
        parted, keys = df.repartition(partitions, "model"), ["partition", "model"]
    else:
        parted, keys = df.repartition(partitions), ["partition"]
    result = parted.withColumn("partition", F.spark_partition_id()).groupBy(*keys).count().collect()
    counts = [0] * partitions
    models: dict[int, list[tuple[str, int]]] = defaultdict(list)
    for r in result:
        counts[r["partition"]] += r["count"]
        if strategy == BY_MODEL:
            models[r["partition"]].append((r["model"], r["count"]))
    for listed in models.values():
        listed.sort(key=lambda m: -m[1])
    return counts, dict(models)


# ------------------------------------------------------------------ main ----


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Parte B: balanceo de carga")
    parser.add_argument("--input", type=Path, default=Path("data/raw"), help="carpeta con CSV")
    parser.add_argument("--output", type=Path, default=Path("output/part_b"))
    parser.add_argument(
        "--partitions",
        type=int,
        nargs="+",
        default=list(DEFAULT_PARTITIONS),
        help="números de particiones a comparar",
    )
    parser.add_argument(
        "--repeats", type=int, default=5, help="mediciones por número de particiones"
    )
    parser.add_argument(
        "--detail", type=int, default=8, help="particiones del reparto que se muestra fila a fila"
    )
    args = parser.parse_args(argv)
    if args.detail not in args.partitions:
        args.partitions.append(args.detail)

    spark = SparkSession.builder.appName("parte-b-balanceo").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    files = list_inputs(args.input)
    groups = group_by_header(files)
    args.output.mkdir(parents=True, exist_ok=True)

    rows = load_rows(sc, groups).persist(StorageLevel.MEMORY_AND_DISK)
    total_rows = rows.count()
    cores = sc.defaultParallelism
    print(
        f"Spark {spark.version} en {sc.master} · {cores} núcleos en los executors · "
        f"{len(files)} archivos · {total_rows:,} filas · "
        f"{rows.getNumPartitions()} particiones de entrada"
    )

    # 1) Tiempos de la Parte A según el número de particiones -------------------
    print(
        f"\nMidiendo la Parte A con {args.partitions} particiones, {args.repeats} veces cada una:"
    )
    runs = measure_times(rows, args.partitions, args.repeats)
    times = summarize_times(runs, args.partitions)
    time_headers = [
        "partitions",
        "runs",
        "median_s",
        "min_s",
        "max_s",
        "stdev_s",
        "median_by_model_s",
        "median_by_serial_s",
        "speedup",
    ]
    time_rows = [
        [t[h] if isinstance(t[h], int) else round(t[h], 3) for h in time_headers] for t in times
    ]
    write_csv(args.output / "tiempos.csv", list(runs[0]), [r.values() for r in runs])
    write_csv(args.output / "tiempos_resumen.csv", time_headers, time_rows)
    print_table(
        time_headers,
        time_rows,
        f"Tiempos de la Parte A (s), mediana de {args.repeats} repeticiones; "
        f"speedup respecto a {args.partitions[0]} particiones:",
    )

    # 2) Concentración de filas por modelo -------------------------------------
    by_model = {m: dd for m, (dd, _) in drive_days_by_model(rows).collect()}
    conc = concentration(by_model)
    ranked = sorted(by_model.items(), key=lambda kv: -kv[1])
    cumulative = 0
    model_rows = []
    for model, n in ranked:
        cumulative += n
        model_rows.append(
            [model, n, round(100 * n / total_rows, 2), round(100 * cumulative / total_rows, 2)]
        )
    write_csv(
        args.output / "filas_por_modelo.csv",
        ["model", "rows", "pct_rows", "cumulative_pct"],
        model_rows,
    )
    print_table(
        ["model", "rows", "pct_rows", "cumulative_pct"],
        model_rows[:10],
        f"Los 10 modelos con más filas (de {conc['keys']}):",
    )
    print(
        "Concentración: "
        + ", ".join(f"top {k} = {v:.1f} %" for k, v in conc["top_share_pct"].items())
        + " · modelos para llegar al "
        + ", ".join(f"{p} %: {k}" for p, k in conc["keys_for_pct"].items())
    )

    # 3) Filas por partición: por modelo vs. sin clave ---------------------------
    df = load_dataframe(spark, groups).select("model").persist(StorageLevel.MEMORY_AND_DISK)
    df.count()
    largest = ranked[0][1]
    skew_headers = [
        "strategy",
        "partitions",
        "empty",
        "min_rows",
        "max_rows",
        "mean_rows",
        "max_over_mean",
        "floor_by_key",
        "cv",
    ]
    skew_rows, partition_rows, detail = [], [], {}
    for n in args.partitions:
        for strategy in (BY_MODEL, NO_KEY):
            counts, models = rows_per_partition(df, n, strategy)
            stats = partition_stats(counts)
            floor = key_skew_floor(largest, total_rows, n) if strategy == BY_MODEL else 1.0
            skew_rows.append(
                [
                    strategy,
                    n,
                    stats["empty"],
                    stats["min_rows"],
                    stats["max_rows"],
                    round(stats["mean_rows"]),
                    round(stats["max_over_mean"], 2),
                    round(floor, 2),
                    round(stats["cv"], 2),
                ]
            )
            for pid, c in enumerate(counts):
                names = "; ".join(m for m, _ in models.get(pid, []))
                partition_rows.append([strategy, n, pid, c, len(models.get(pid, [])), names])
            if n == args.detail:
                detail[strategy] = (counts, models)
    write_csv(args.output / "sesgo_resumen.csv", skew_headers, skew_rows)
    write_csv(
        args.output / "filas_por_particion.csv",
        ["strategy", "partitions", "partition", "rows", "models", "model_names"],
        partition_rows,
    )
    print_table(
        skew_headers,
        skew_rows,
        "Filas por partición (max_over_mean = 1 es reparto perfecto; floor_by_key es el mínimo "
        "posible por clave, fijado por el modelo más grande):",
    )

    keyed, keyed_models = detail[BY_MODEL]
    unkeyed, _ = detail[NO_KEY]
    detail_rows = []
    for pid in range(args.detail):
        listed = keyed_models.get(pid, [])
        biggest = (
            f"{listed[0][0]} ({100 * listed[0][1] / max(keyed[pid], 1):.0f} %)" if listed else "-"
        )
        detail_rows.append(
            [
                pid,
                keyed[pid],
                f"{100 * keyed[pid] / total_rows:.1f}%",
                len(listed),
                biggest,
                unkeyed[pid],
                f"{100 * unkeyed[pid] / total_rows:.1f}%",
            ]
        )
    print_table(
        [
            "partition",
            "rows_by_model",
            "pct",
            "models",
            "largest_model (pct of partition)",
            "rows_no_key",
            "pct_no_key",
        ],
        detail_rows,
        f"Detalle con {args.detail} particiones (spark_partition_id()):",
    )

    summary = {
        "input": {"files": [p.name for p in files], "rows": total_rows},
        "spark": {
            "version": spark.version,
            "master": sc.master,
            "cores": cores,
            "input_partitions": rows.getNumPartitions(),
        },
        "timings": times,
        "concentration": conc,
        "skew": [dict(zip(skew_headers, r, strict=True)) for r in skew_rows],
    }
    (args.output / "resumen.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(
        f"\nResultados en {args.output}/: tiempos.csv, tiempos_resumen.csv, filas_por_modelo.csv, "
        "sesgo_resumen.csv, filas_por_particion.csv, resumen.json"
    )

    spark.stop()
    return 0
