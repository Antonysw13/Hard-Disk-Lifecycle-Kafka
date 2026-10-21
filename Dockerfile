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

# Conector de Kafka para Structured Streaming (Parte C). PySpark no lo trae: se añaden sus jars
# (y los de sus dependencias) a los de Spark, con checksum. Las versiones salen del POM de
# spark-sql-kafka-0-10 4.2.0: si cambias la versión de PySpark en uv.lock, cámbialas también.
ARG SPARK_JARS=/opt/venv/lib/python3.12/site-packages/pyspark/jars
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 --checksum=sha256:64539c45e5d39f7ddb747312c239f38f5208d08c1a0ddf7d70beb2fbd9373705 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.0/spark-sql-kafka-0-10_2.13-4.2.0.jar ${SPARK_JARS}/
ADD --chmod=644 --checksum=sha256:641c7947afc8aa1189e433b7c2d305293993c8691bc1c2062d6d4f513b12518c \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.0/spark-token-provider-kafka-0-10_2.13-4.2.0.jar ${SPARK_JARS}/
ADD --chmod=644 --checksum=sha256:014b4ef36884478a932641818b080c36a8f8e2e92dc771e800c9d2694f4f83d8 \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar ${SPARK_JARS}/
ADD --chmod=644 --checksum=sha256:f77a5060d6936a9144023584232f9de3f2248f1abfe156e9d31795f18770674f \
    ${MAVEN}/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar ${SPARK_JARS}/

COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# PySpark trae Spark completo (jars y scripts spark-class / spark-submit).
# PYTHONPATH=/app: driver y executors importan los paquetes part_a, part_b y part_c.
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
