#!/bin/sh
set -eu

# Antes de lanzar un job, valida la muestra de data/raw contra manifest.json y descarga
# lo que falte o esté dañado. La muestra la definen DS_QUARTER, DS_START y DS_DAYS (.env).
# Si la validación falla, el job no arranca.
python -m drivestats.download
echo

exec "$@"
