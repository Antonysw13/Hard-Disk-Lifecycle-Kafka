"""Valida la muestra de Backblaze Drive Stats y descarga los días que falten.

La muestra son N días consecutivos de un trimestre. El ZIP del trimestre pesa unos 2 GB,
pero el ejercicio solo necesita 7 a 14 archivos diarios. En vez de bajarlo entero, se lee su
directorio central con peticiones HTTP Range (Backblaze B2 las acepta) y luego se descarga y
descomprime solo cada CSV pedido.

Cada CSV descargado queda registrado en `manifest.json` (en la misma carpeta) con el tamaño y
el CRC32 que indica el ZIP. En cada ejecución se recalcula el CRC32 de los archivos locales
(menos de un segundo por GB): los que falten o no coincidan se vuelven a descargar. Si todo es
válido, no se toca la red.

Uso:
    python -m drivestats.download                   # muestra de DS_QUARTER, DS_START, DS_DAYS
    python -m drivestats.download --quarter Q2_2026 --days 14
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import re
import struct
import sys
import time
import urllib.request
import zipfile
import zlib
from pathlib import Path

BASE_URL = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data"
CHUNK = 1 << 20  # 1 MiB
RETRIES = 3
TIMEOUT_S = 60
MANIFEST_NAME = "manifest.json"

_LOCAL_HEADER = struct.Struct("<IHHHHHIIIHH")  # cabecera local de una entrada ZIP (30 bytes)
_LOCAL_HEADER_SIGNATURE = 0x04034B50

# Estado de cada archivo de la muestra en disco
OK, MISSING, CORRUPT, UNVERIFIED = "ok", "missing", "corrupt", "unverified"
STATUS_TEXT = {
    OK: "✓ {name}: válido (tamaño y CRC32 coinciden con {manifest})",
    MISSING: "↓ {name}: no está, se descargará",
    CORRUPT: "✗ {name}: tamaño o CRC32 no coinciden con {manifest}, se descargará de nuevo",
    UNVERIFIED: "? {name}: no está en {manifest}, se cotejará con el índice del ZIP",
}


# ------------------------------------------------------------ acceso remoto ----


class HTTPRangeFile(io.RawIOBase):
    """Archivo remoto de solo lectura y con `seek`, servido con peticiones HTTP Range."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.pos = 0
        request = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(request, timeout=TIMEOUT_S) as resp:
            if resp.headers.get("Accept-Ranges") != "bytes":
                raise RuntimeError(f"{url} no acepta peticiones Range")
            self.size = int(resp.headers["Content-Length"])

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self.pos, io.SEEK_END: self.size}[whence]
        self.pos = base + offset
        return self.pos

    def readinto(self, buffer) -> int:  # type: ignore[override]
        if self.pos >= self.size:
            return 0
        end = min(self.pos + len(buffer), self.size) - 1
        data = fetch_range(self.url, self.pos, end)
        buffer[: len(data)] = data
        self.pos += len(data)
        return len(data)


def fetch_range(url: str, start: int, end: int) -> bytes:
    req = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
        if resp.status != 206:
            raise RuntimeError(f"Se esperaba HTTP 206 y llegó {resp.status}")
        return resp.read()


def remote_index(url: str) -> dict[str, zipfile.ZipInfo]:
    """Lee el directorio central del ZIP remoto: nombre → tamaño, CRC32 y posición."""
    remote = HTTPRangeFile(url)
    with zipfile.ZipFile(io.BufferedReader(remote, buffer_size=CHUNK)) as archive:
        return {Path(i.filename).name: i for i in archive.infolist() if not i.is_dir()}


def download_entry(url: str, info: zipfile.ZipInfo, dest: Path) -> None:
    """Baja una entrada del ZIP con una sola petición Range y la descomprime en streaming."""
    header = fetch_range(url, info.header_offset, info.header_offset + _LOCAL_HEADER.size - 1)
    fields = _LOCAL_HEADER.unpack(header)
    if fields[0] != _LOCAL_HEADER_SIGNATURE:
        raise RuntimeError(f"Cabecera local inválida para {info.filename}")
    name_len, extra_len = fields[9], fields[10]
    data_start = info.header_offset + _LOCAL_HEADER.size + name_len + extra_len
    data_end = data_start + info.compress_size - 1

    if info.compress_type == zipfile.ZIP_DEFLATED:
        decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
    elif info.compress_type == zipfile.ZIP_STORED:
        decompressor = None
    else:
        raise RuntimeError(f"Compresión no soportada ({info.compress_type}) en {info.filename}")

    tmp = dest.with_suffix(dest.suffix + ".part")
    crc, written = 0, 0
    req = urllib.request.Request(url, headers={"Range": f"bytes={data_start}-{data_end}"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp, tmp.open("wb") as out:
        while chunk := resp.read(CHUNK):
            if decompressor is not None:
                chunk = decompressor.decompress(chunk)
            crc = zlib.crc32(chunk, crc)
            written += len(chunk)
            out.write(chunk)
        if decompressor is not None:
            tail = decompressor.flush()
            crc = zlib.crc32(tail, crc)
            written += len(tail)
            out.write(tail)

    if written != info.file_size or crc != info.CRC:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"{info.filename}: tamaño o CRC no coinciden")
    tmp.replace(dest)


def download_with_retries(url: str, info: zipfile.ZipInfo, dest: Path) -> bool:
    for attempt in range(1, RETRIES + 1):
        t0 = time.perf_counter()
        try:
            download_entry(url, info, dest)
        except (OSError, RuntimeError) as exc:
            print(f"  {dest.name}: intento {attempt}/{RETRIES} falló: {exc}", file=sys.stderr)
            if attempt < RETRIES:
                time.sleep(2 * attempt)
        else:
            print(
                f"  ↓ {dest.name}: {info.compress_size / 1e6:.1f} MB descargados → "
                f"{info.file_size / 1e6:.1f} MB ({time.perf_counter() - t0:.1f} s)",
                flush=True,
            )
            return True
    return False


# ------------------------------------------------------- validación local ----


def file_crc32(path: Path) -> int:
    crc = 0
    with path.open("rb") as fh:
        while chunk := fh.read(4 * CHUNK):
            crc = zlib.crc32(chunk, crc)
    return crc


def matches(path: Path, size: int, crc32: int) -> bool:
    return path.stat().st_size == size and file_crc32(path) == crc32


def load_manifest(folder: Path) -> dict[str, dict]:
    try:
        return json.loads((folder / MANIFEST_NAME).read_text())
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        print(f"  {MANIFEST_NAME} ilegible: se ignora y se verifica todo", file=sys.stderr)
        return {}


def save_manifest(folder: Path, manifest: dict[str, dict]) -> None:
    tmp = folder / f"{MANIFEST_NAME}.tmp"
    tmp.write_text(json.dumps(dict(sorted(manifest.items())), indent=2) + "\n")
    tmp.replace(folder / MANIFEST_NAME)


def manifest_entry(info: zipfile.ZipInfo, source: str) -> dict:
    return {"size": info.file_size, "crc32": info.CRC, "source": source}


def local_status(folder: Path, name: str, manifest: dict[str, dict]) -> str:
    path = folder / name
    if not path.is_file():
        return MISSING
    entry = manifest.get(name)
    if entry is None:
        return UNVERIFIED
    return OK if matches(path, entry["size"], entry["crc32"]) else CORRUPT


# ------------------------------------------------------------------ CLI ----


def quarter_start(quarter: str) -> dt.date:
    match = re.fullmatch(r"Q([1-4])_(\d{4})", quarter)
    if not match:
        raise argparse.ArgumentTypeError(f"Trimestre inválido: {quarter!r} (ej. Q2_2026)")
    q, year = int(match[1]), int(match[2])
    return dt.date(year, 3 * (q - 1) + 1, 1)


def parse_quarter(value: str) -> str:
    quarter_start(value)
    return value


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("debe ser 1 o más")
    return number


def wanted_names(start: dt.date, days: int) -> list[str]:
    return [f"{start + dt.timedelta(days=i):%Y-%m-%d}.csv" for i in range(days)]


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    env = os.environ
    parser = argparse.ArgumentParser(
        prog="python -m drivestats.download", description=__doc__.splitlines()[0]
    )
    parser.add_argument(
        "--quarter",
        type=parse_quarter,
        default=env.get("DS_QUARTER") or "Q2_2026",
        help="trimestre del ZIP, ej. Q2_2026 (variable DS_QUARTER)",
    )
    parser.add_argument(
        "--days",
        type=positive_int,
        default=env.get("DS_DAYS") or "7",
        help="número de días consecutivos (variable DS_DAYS)",
    )
    parser.add_argument(
        "--start",
        type=dt.date.fromisoformat,
        default=env.get("DS_START") or None,
        help="primer día AAAA-MM-DD; por defecto, el primero del trimestre (variable DS_START)",
    )
    parser.add_argument("--out", type=Path, default=Path("data/raw"), help="carpeta de destino")
    return parser.parse_args(argv)


def warn_extra_files(folder: Path, names: list[str]) -> None:
    extra = sorted(p.name for p in folder.glob("*.csv") if p.name not in names)
    if extra:
        listed = ", ".join(extra) if len(extra) <= 3 else f"{extra[0]} … {extra[-1]}"
        print(
            f"Aviso: {folder} tiene {len(extra)} CSV fuera de la muestra ({listed}). "
            "Los jobs leen todos los CSV de la carpeta: bórralos si no quieres incluirlos."
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    start = args.start or quarter_start(args.quarter)
    names = wanted_names(start, args.days)
    folder = args.out
    folder.mkdir(parents=True, exist_ok=True)
    for leftover in folder.glob("*.part"):  # descargas interrumpidas
        leftover.unlink()

    span = f"{names[0]} … {names[-1]} ({len(names)} {'día' if len(names) == 1 else 'días'})"
    print(f"Validando la muestra {args.quarter} en {folder}: {span}")
    manifest = load_manifest(folder)
    status = {name: local_status(folder, name, manifest) for name in names}
    for name in names:
        print("  " + STATUS_TEXT[status[name]].format(name=name, manifest=MANIFEST_NAME))

    pending = [n for n in names if status[n] != OK]
    if not pending:
        warn_extra_files(folder, names)
        print(f"Muestra válida: {len(names)} de {len(names)} archivos, no hace falta descargar.")
        return 0

    zip_name = f"data_{args.quarter}.zip"
    url = f"{BASE_URL}/{zip_name}"
    print(f"Leyendo el índice de {url}")
    try:
        entries = remote_index(url)
    except (OSError, RuntimeError) as exc:
        needed = [n for n in pending if status[n] != UNVERIFIED]
        if needed:
            print(
                f"No se pudo leer el índice ({exc}) y faltan o están dañados: {', '.join(needed)}",
                file=sys.stderr,
            )
            return 1
        print(f"Aviso: sin acceso a Backblaze ({exc}); se usan sin verificar: {', '.join(pending)}")
        warn_extra_files(folder, names)
        return 0

    not_found = [n for n in pending if n not in entries]
    if not_found:
        print(f"No están en {zip_name}: {', '.join(not_found)}", file=sys.stderr)
        return 1

    for name in pending:
        info, path = entries[name], folder / name
        if status[name] == UNVERIFIED and matches(path, info.file_size, info.CRC):
            print(f"  ✓ {name}: coincide con el ZIP, se registra en {MANIFEST_NAME}")
        elif not download_with_retries(url, info, path):
            return 1
        manifest[name] = manifest_entry(info, zip_name)
        save_manifest(folder, manifest)

    warn_extra_files(folder, names)
    print(f"Muestra lista en {folder}: {span}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
