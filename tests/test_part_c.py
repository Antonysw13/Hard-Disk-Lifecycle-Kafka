import datetime as dt
import json
import random
import string

import pytest

from part_c.events import (
    DEFAULT_MODELS,
    END_MODEL,
    EPOCH,
    encode,
    end_marker,
    end_marker_date,
    in_bucket,
    window_offset_days,
    window_starts,
    windows_containing,
)
from part_c.producer import models_arg, parse_models, read_day, split_row
from part_c.streaming import compare, late_by_window, span

D = dt.date.fromisoformat
APRIL_1 = D("2026-04-01")


def test_encode_is_json_even_with_characters_that_need_escaping():
    plain = json.loads(encode("2026-04-01", "ZLW1AHGN", "ST12000NM001G", 1))
    assert plain == {
        "date": "2026-04-01",
        "serial_number": "ZLW1AHGN",
        "model": "ST12000NM001G",
        "failure": 1,
    }
    tricky = json.loads(encode("2026-04-01", 'A"B\\C', "X\tY", 0))
    assert (tricky["serial_number"], tricky["model"]) == ('A"B\\C', "X\tY")


def test_end_marker_uses_the_reserved_model():
    assert json.loads(end_marker(D("2026-04-16")))["model"] == END_MODEL


def test_in_bucket_is_deterministic_and_selects_about_the_requested_share():
    rng = random.Random(1)
    serials = [
        "".join(rng.choices(string.ascii_uppercase + string.digits, k=10)) for _ in range(20_000)
    ]
    picked = [s for s in serials if in_bucket(s, 2)]
    assert picked == [s for s in serials if in_bucket(s, 2)]
    assert 0.015 < len(picked) / len(serials) < 0.025
    assert all(in_bucket(s, 100) for s in serials[:100])
    # La sal cambia qué discos se eligen (retraso independiente del muestreo)
    assert picked != [s for s in serials if in_bucket(s, 2, "late:")]


@pytest.mark.parametrize("slide", [1, 3, 7])
def test_window_offset_aligns_a_window_with_the_first_day(slide):
    offset = window_offset_days(APRIL_1, slide)
    assert 0 <= offset < slide
    assert ((APRIL_1 - EPOCH).days - offset) % slide == 0


@pytest.mark.parametrize(
    ("days", "window", "slide", "expected"),
    [
        (7, 7, 7, ["2026-04-01"]),
        (14, 7, 7, ["2026-04-01", "2026-04-08"]),
        (7, 3, 3, ["2026-04-01", "2026-04-04", "2026-04-07"]),
    ],
)
def test_window_starts_tumbling(days, window, slide, expected):
    last = APRIL_1 + dt.timedelta(days=days - 1)
    assert [s.isoformat() for s in window_starts(APRIL_1, last, window, slide)] == expected


def test_window_starts_sliding_includes_windows_that_start_before_the_data():
    starts = window_starts(APRIL_1, D("2026-04-07"), 7, 1)
    assert starts[0] == D("2026-03-26") and starts[-1] == D("2026-04-07")
    assert len(starts) == 13


def test_windows_containing():
    assert windows_containing(D("2026-04-09"), APRIL_1, 7, 7) == [D("2026-04-08")]
    assert len(windows_containing(D("2026-04-03"), APRIL_1, 7, 1)) == 7


@pytest.mark.parametrize(("window", "slide", "watermark"), [(7, 7, 2), (3, 3, 4), (7, 1, 0)])
def test_end_marker_closes_every_window(window, slide, watermark):
    last = D("2026-04-07")
    marker = end_marker_date(last, window, watermark)
    watermark_after_marker = marker - dt.timedelta(days=watermark)
    for start in window_starts(APRIL_1, last, window, slide):
        assert start + dt.timedelta(days=window) <= watermark_after_marker


def test_parse_models():
    assert parse_models("") == DEFAULT_MODELS
    assert parse_models("all") == ()
    assert parse_models(" A , B ,") == ("A", "B")
    assert parse_models(models_arg(DEFAULT_MODELS)) == DEFAULT_MODELS
    assert parse_models(models_arg(())) == ()


def test_split_row_matches_csv_and_handles_quotes():
    assert split_row("2026-04-01,S1,M,123,0,dc,1,2,3\n", 4)[:5] == [
        "2026-04-01",
        "S1",
        "M",
        "123",
        "0",
    ]
    assert split_row('2026-04-01,S1,"M, raro",123,1\n', 4) == [
        "2026-04-01",
        "S1",
        "M, raro",
        "123",
        "1",
    ]


def test_read_day_filters_models_and_invalid_rows(tmp_path):
    path = tmp_path / "2026-04-01.csv"
    path.write_text(
        "date,serial_number,model,capacity_bytes,failure,smart_1_raw\n"
        "2026-04-01,S1,A,1,0,x\n"
        "2026-04-01,S2, B ,1,1,x\n"  # modelo con espacios: se recorta, como en la Parte A
        "2026-04-01,S3,C,1,0,x\n"  # modelo no pedido
        "2026-04-01,S4,A,1,,x\n"  # falla vacía: fila inválida
        "2026-04-01,S5,A\n"  # fila corta
    )
    assert list(read_day(path, ("A", "B"), 100)) == [
        ("2026-04-01", "S1", "A", 0),
        ("2026-04-01", "S2", "B", 1),
    ]
    assert len(list(read_day(path, (), 100))) == 3


def test_late_by_window_maps_late_events_to_their_windows():
    report = {"late_by_date_model": {"2026-04-03": {"A": 5}, "2026-04-09": {"A": 2, "B": 1}}}
    assert late_by_window(report, APRIL_1, 7, 7) == {
        ("2026-04-01", "A"): 5,
        ("2026-04-08", "A"): 2,
        ("2026-04-08", "B"): 1,
    }


def test_compare_statuses():
    batch = {("w1", "A"): (100, 3), ("w1", "B"): (50, 1), ("w2", "A"): (80, 0), ("w2", "B"): (9, 0)}
    stream = {
        ("w1", "A"): (100, 3),
        ("w1", "B"): (45, 1),
        ("w2", "A"): (70, 0),
        ("w2", "B"): (10, 0),
    }
    late = {("w1", "B"): 5, ("w2", "A"): 4}
    status = {(r["window_start"], r["model"]): r["status"] for r in compare(stream, batch, late)}
    assert status == {
        ("w1", "A"): "igual",
        ("w1", "B"): "tardíos descartados",  # faltan 5 y hubo 5 tardíos
        ("w2", "A"): "error",  # faltan 10 pero solo 4 llegaron tarde
        ("w2", "B"): "error",  # el streaming no puede contar más que el batch
    }


def test_span_shows_the_last_day_of_the_window():
    assert span("2026-04-01", "2026-04-08") == "2026-04-01 … 2026-04-07"
