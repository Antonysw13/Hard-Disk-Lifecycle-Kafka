#!/bin/sh
set -eu

# Cada nodo de Spark se anuncia con la IP de su contenedor. Así los executors pueden
# conectarse al driver aunque el hostname del contenedor sea un id aleatorio.
if [ -z "${SPARK_LOCAL_HOSTNAME:-}" ]; then
    SPARK_LOCAL_HOSTNAME="$(hostname -i | cut -d' ' -f1)"
    export SPARK_LOCAL_HOSTNAME
fi

exec "$@"
