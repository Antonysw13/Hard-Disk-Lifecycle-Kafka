import datetime as dt
import json
import zipfile
import zlib

import pytest

from drivestats import download
from drivestats.download import (
    CORRUPT,
    MANIFEST_NAME,
    MISSING,
    OK,
    UNVERIFIED,
    local_status,
    main,
    parse_args,
    quarter_start,
    wanted_names,
)

CONTENT = b"date,serial_number,model,failure\n2026-04-01,S1,A,0\n"


def write_sample(folder, names, content=CONTENT):
    folder.mkdir(parents=True, exist_ok=True)
    for name in names:
        (folder / name).write_bytes(content)


def write_manifest(folder, names, crc32=None, content=CONTENT):
    entry = {"size": len(content), "crc32": crc32 if crc32 is not None else zlib.crc32(content)}
    (folder / MANIFEST_NAME).write_text(json.dumps({n: entry for n in names}))


def fake_index(names, content=CONTENT):
    entries = {}
    for name in names:
        info = zipfile.ZipInfo(f"data_Q2_2026/{name}")
        info.file_size, info.CRC = len(content), zlib.crc32(content)
        entries[name] = info
    return entries


def run(folder, days=2):
    return main(["--quarter", "Q2_2026", "--days", str(days), "--out", str(folder)])


def no_network(_url):
    raise AssertionError("no debía tocar la red")


def offline(_url):
    raise OSError("sin red")


def test_sample_names_start_on_first_day_of_quarter():
    assert quarter_start("Q2_2026").isoformat() == "2026-04-01"
    assert wanted_names(quarter_start("Q4_2025"), 3) == [
        "2025-10-01.csv",
        "2025-10-02.csv",
        "2025-10-03.csv",
    ]


def test_sample_comes_from_environment(monkeypatch):
    monkeypatch.setenv("DS_QUARTER", "Q1_2026")
    monkeypatch.setenv("DS_START", "2026-02-10")
    monkeypatch.setenv("DS_DAYS", "14")
    args = parse_args([])
    assert (args.quarter, args.start, args.days) == ("Q1_2026", dt.date(2026, 2, 10), 14)


def test_empty_environment_uses_defaults(monkeypatch):
    for var in ("DS_QUARTER", "DS_START", "DS_DAYS"):
        monkeypatch.setenv(var, "")
    args = parse_args([])
    assert (args.quarter, args.start, args.days) == ("Q2_2026", None, 7)


def test_local_status(tmp_path):
    write_sample(tmp_path, ["ok.csv", "corrupt.csv", "unverified.csv"])
    (tmp_path / "corrupt.csv").write_bytes(CONTENT[:-5])  # truncado
    write_manifest(tmp_path, ["ok.csv", "corrupt.csv"])
    manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
    assert local_status(tmp_path, "ok.csv", manifest) == OK
    assert local_status(tmp_path, "corrupt.csv", manifest) == CORRUPT
    assert local_status(tmp_path, "unverified.csv", manifest) == UNVERIFIED
    assert local_status(tmp_path, "missing.csv", manifest) == MISSING


def test_valid_sample_does_not_touch_the_network(tmp_path, monkeypatch):
    names = wanted_names(dt.date(2026, 4, 1), 2)
    write_sample(tmp_path, names)
    write_manifest(tmp_path, names)
    (tmp_path / "2026-04-03.csv.part").write_bytes(b"medio archivo")
    monkeypatch.setattr(download, "remote_index", no_network)
    assert run(tmp_path) == 0
    assert not (tmp_path / "2026-04-03.csv.part").exists()


def test_unverified_files_matching_the_zip_are_registered(tmp_path, monkeypatch):
    names = wanted_names(dt.date(2026, 4, 1), 2)
    write_sample(tmp_path, names)
    monkeypatch.setattr(download, "remote_index", lambda _url: fake_index(names))
    monkeypatch.setattr(download, "download_with_retries", no_network)
    assert run(tmp_path) == 0
    manifest = json.loads((tmp_path / MANIFEST_NAME).read_text())
    assert sorted(manifest) == names
    assert manifest[names[0]]["source"] == "data_Q2_2026.zip"


def test_missing_and_corrupt_files_are_downloaded(tmp_path, monkeypatch):
    names = wanted_names(dt.date(2026, 4, 1), 2)
    write_sample(tmp_path, names[:1], content=b"basura")
    write_manifest(tmp_path, names[:1])
    downloaded = []

    def fake_download(_url, _info, dest):
        dest.write_bytes(CONTENT)
        downloaded.append(dest.name)
        return True

    monkeypatch.setattr(download, "remote_index", lambda _url: fake_index(names))
    monkeypatch.setattr(download, "download_with_retries", fake_download)
    assert run(tmp_path) == 0
    assert downloaded == names
    assert all((tmp_path / n).read_bytes() == CONTENT for n in names)


@pytest.mark.parametrize(("present", "expected"), [(2, 0), (1, 1)])
def test_offline(tmp_path, monkeypatch, present, expected):
    # Sin red: los archivos sin registro se aceptan con aviso; si falta alguno, error.
    names = wanted_names(dt.date(2026, 4, 1), 2)
    write_sample(tmp_path, names[:present])
    monkeypatch.setattr(download, "remote_index", offline)
    assert run(tmp_path) == expected
