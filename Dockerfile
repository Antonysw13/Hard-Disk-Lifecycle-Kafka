# syntax=docker/dockerfile:1
# Imagen única para todo el proyecto: master, workers, driver y descargador.
# Usar la misma imagen en todos los nodos garantiza las mismas versiones de Python,
# Java y PySpark en el driver y en los executors.
FROM python:3.12-slim-bookworm

# Java 21 (Spark 4 necesita Java 17 o superior) y uv, copiados de sus imágenes oficiales
COPY --from=eclipse-temurin:21-jre /opt/java/openjdk /opt/java/openjdk
COPY --from=ghcr.io/astral-sh/uv:0.12.18 /uv /uvx /bin/

ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/venv/bin:/opt/java/openjdk/bin:$PATH \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias primero: esta capa solo se reconstruye cuando cambia uv.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project

COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# PySpark trae Spark completo (jars y scripts spark-class / spark-submit).
# PYTHONPATH=/app: driver y executors importan los paquetes part_a y part_b del proyecto.
ENV SPARK_HOME=/opt/venv/lib/python3.12/site-packages/pyspark \
    SPARK_CONF_DIR=/app/conf \
    SPARK_WORKER_DIR=/tmp/spark-worker \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    PYTHONPATH=/app

# Usuario sin privilegios; el UID coincide con el del host para que ./output sea tuyo
ARG UID=1000
RUN useradd --uid "${UID}" --create-home spark \
    && install -m 0755 docker/entrypoint.sh docker/with-data.sh /usr/local/bin/
USER spark

ENTRYPOINT ["entrypoint.sh"]
CMD ["bash"]
