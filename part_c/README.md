# Parte C: Streaming con Kafka

> Un producer publica en Kafka los registros día por día, con reloj acelerado (un día cada
> 10 s), filtrando de 3 a 5 modelos. Structured Streaming agrupa por modelo en ventanas de
> 7 días con watermark de 2 días, y calcula días-disco, fallas y tasa anualizada por ventana.

```bash
docker compose run --rm part-c          # Kafka + Spark + producer; termina solo (~3 min)
docker compose down                     # apaga el clúster y Kafka
```

En el pipeline completo (`docker compose up`) es la última etapa, `pipeline-c`: arranca cuando
la Parte B terminó bien y usa las opciones por defecto de `.env`.

La primera vez además descarga la imagen de Kafka (`apache/kafka:4.3.1`).

## Arquitectura

```text
 data/raw/*.csv ──► producer ──JSON──► Kafka (topic «drivestats», 4 particiones, clave = serial)
 (un día por tic)   (Python,                       │
                    confluent-kafka)               ▼
                              Spark Structured Streaming (driver en part-c, executors en workers)
                              from_json → event_time → withWatermark(2 días)
                              → groupBy(window(7 días), model) → count, sum(failure)
                                   │                                  │
                         «en_vivo» (update)                 «ventanas_cerradas» (append)
                         ventanas que cambian               ventanas que el watermark cerró
                                   │                                  │
                                   └──────► output/part_c/ ◄──────────┘
                                            + verificación contra batch
```

| Servicio | Qué hace |
| -------- | -------- |
| `kafka` | Broker de un nodo en modo KRaft (sin ZooKeeper). Se llega por `kafka:9092` desde la red de Compose |
| `part-c` | Valida la muestra, vacía el topic, arranca las dos consultas, lanza el producer, espera a que se cierren todas las ventanas y verifica |
| `part-c-producer` | El mismo producer, suelto, para publicar desde otro terminal (ver más abajo) |

El conector de Kafka para Spark (`spark-sql-kafka-0-10` y sus dependencias) no viene con
PySpark: el `Dockerfile` añade sus 4 jars a los de Spark, con checksum SHA-256.

## Qué pasa, paso a paso

### 1. El producer: un día cada 10 s

`part_c/producer.py` recorre los CSV de `data/raw` en orden de fecha. En cada **tic** (10 s
reales = 1 día de datos):

1. Lee el CSV del día y se queda con las filas de los **5 modelos** por defecto: unos 120.000
   eventos por día, frente a los ~346.000 de todos los modelos. Las reglas de validez son las de
   la Parte A (modelo no vacío, `failure` 0 o 1).
2. Un **2 % de los discos** (elegidos por hash del serial, siempre los mismos) se guarda y se
   publica **un día después**, con su fecha original: simula reportes que llegan tarde.
3. Publica cada fila como JSON con clave = `serial_number`. Kafka reparte por hash de la clave,
   así que las 4 particiones quedan equilibradas (con clave = modelo saldría el mismo sesgo que
   en la Parte B).
4. Espera hasta el siguiente tic. Si va tarde lo avisa.

Al acabar los datos publica los eventos tardíos que queden y, por último, una **marca de fin**:
un evento sin datos (`model = "__fin__"`) fechado 9 días después del último día. Su único
efecto es hacer avanzar el watermark para que Spark cierre las últimas ventanas.

Los 5 modelos tienen muchos discos y AFR distintas en la Parte A, de 0,46 % a 11,97 %:
WDC WUH722222ALE6L4, TOSHIBA MG07ACA14TA, ST12000NM0008, HGST HUH721212ALN604 y ST24000NM002H.

### 2. Tiempo de evento, ventanas y watermark

```python
events = (spark.readStream.format("kafka").option("subscribe", "drivestats").load()
          .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
          .select("e.*")
          .withColumn("event_time", F.to_timestamp("date", "yyyy-MM-dd"))
          .withWatermark("event_time", "2 days"))

counts = events.groupBy(F.window("event_time", "7 days", "7 days", f"{offset} days"), "model")
               .agg(F.count("*").alias("drive_days"), F.sum("failure").alias("failures"))
```

- **Tiempo de evento, no de llegada.** La ventana de un evento la decide su campo `date`, no
  el momento en que llega a Kafka. Un evento del 3 de abril que llega el 4 va a la ventana
  del 3.
- **Ventanas alineadas a la muestra.** Spark alinea las ventanas al 1970-01-01 (un jueves).
  Con el desplazamiento `offset` (`startTime`), la primera ventana empieza el primer día de los
  datos (1 de abril).
- **Watermark de 2 días.** Spark lleva la cuenta de la fecha máxima vista. El watermark es esa
  fecha menos 2 días y significa «ya no espero eventos anteriores a esto». Una ventana se
  **cierra** cuando el watermark pasa su final: a partir de ahí, Spark la emite como definitiva,
  la borra del estado y descarta los eventos que lleguen después para ella.
- **Días-disco y fallas igual que en la Parte A.** `count` y `sum(failure)`; la AFR y su
  intervalo de confianza se calculan al escribir cada ventana.

### 3. Dos consultas, dos modos de salida

| Consulta | Modo | Qué emite | Para qué |
| -------- | ---- | --------- | -------- |
| `en_vivo` | `update` | En cada lote, las ventanas que cambiaron, aunque sigan abiertas | Ver la AFR evolucionar día a día |
| `ventanas_cerradas` | `append` | Cada ventana **una sola vez**, cuando el watermark la cierra | El resultado definitivo |

El modo `append` en una agregación solo es posible con watermark: sin él, Spark nunca sabría
cuándo una ventana está completa. Cada consulta tiene su propio checkpoint en
`output/part_c/checkpoints/` (offsets de Kafka y estado de las ventanas). Ahí escriben también
los executors, y por eso los workers montan `./output`.

El trigger es de 10 s, igual que un día simulado, así que normalmente cada lote trae un día.

### 4. Fin y verificación

El job termina solo cuando `ventanas_cerradas` ha emitido todas las ventanas esperadas.
Después recalcula lo mismo **en batch** con `part_a.mapreduce.load_dataframe` sobre los CSV, con
el mismo filtro de modelos, y compara ventana por ventana y modelo por modelo:

| Estado | Significado |
| ------ | ----------- |
| `igual` | Streaming y batch dan los mismos días-disco y fallas |
| `tardíos descartados` | Al streaming le faltan eventos, pero son menos que los que el producer publicó tarde para esa ventana: los descartó el watermark |
| `error` | Cualquier otra diferencia. El job termina con código 1 |

## Resultados (muestra del 1 al 7 de abril, opciones por defecto)

```text
[ventanas_cerradas] lote 8 · el watermark pasó el final de la ventana: resultado definitivo (modo append):
window                   model                 drive_days  failures  afr_pct  afr_ci95
2026-04-01 … 2026-04-07  HGST HUH721212ALN604      67,082        22    11.97  7.50 – 18.12
2026-04-01 … 2026-04-07  ST12000NM0008            129,193         7     1.98  0.80 – 4.07
2026-04-01 … 2026-04-07  ST24000NM002H             65,308         9     5.03  2.30 – 9.55
2026-04-01 … 2026-04-07  TOSHIBA MG07ACA14TA      259,818        19     2.67  1.61 – 4.17
2026-04-01 … 2026-04-07  WDC WUH722222ALE6L4      319,093         4     0.46  0.12 – 1.17

✓ streaming = batch en los 5 pares (ventana, modelo), incluidos los eventos que llegaron tarde
  dentro del watermark
Eventos: 840,494 publicados por el producer (+1 marca de fin) · 840,495 leídos de Kafka
```

- **El resultado del streaming coincide con el batch y con la Parte A**, que da las mismas
  cifras para estos modelos. Los 16.610 eventos que llegaron con un día de retraso se contaron
  todos: un día es menos que el umbral de 2 días.
- **La AFR en vivo de una ventana abierta es provisional.** La de HGST HUH721212ALN604 pasa
  por 15,98 % (lote 0, 2.284 días-disco y 1 falla), 3,84 %, 7,66 %, 8,42 %, 9,56 % y 12,00 %
  hasta quedar en 11,97 %. Es la sección 7 de la Parte A en movimiento: con poca exposición
  la tasa salta mucho.
- **El watermark avanza con los datos.** Va del 30 de marzo al 5 de abril (fecha máxima vista
  menos 2 días). Solo la marca de fin lo lleva al 14 de abril, y ahí se cierra la ventana.
- **Arranque en frío y puesta al día.** El primer lote tarda unos 27 s, porque Spark compila el
  plan y abre los consumidores de Kafka. El segundo procesa tres días acumulados (330.000
  eventos) en 14 s. Desde el tercero, cada lote es un día (~120.000 eventos) y tarda 5-7 s, por
  debajo del trigger de 10 s: el sistema da abasto.

## Experimento: eventos que llegan después de cerrarse su ventana

Con la muestra de 7 días y ventanas de 7 días no se puede descartar nada: hay una sola
ventana, y el watermark no pasa del 5 de abril hasta la marca de fin. Para ver el descarte hacen
falta ventanas que se cierren a mitad del flujo, así que el experimento usa ventanas de 3 días
(1-3, 4-6 y 7-9 de abril) y retrasos más largos que el umbral de 2 días:

```bash
docker compose run --rm part-c --window-days 3 --late-days 4
docker compose run --rm part-c --window-days 3 --late-days 5
```

| Retraso | Los tardíos del 3 de abril llegan… | Resultado |
| ------- | ---------------------------------- | --------- |
| 4 días | en el tic 7, junto con los datos del 7 de abril, en el **mismo lote** en que el watermark llega al 4 de abril y cierra la ventana 1-3 | Se cuentan: `streaming = batch en los 15 pares` |
| 5 días | en el tic 8, sin datos nuevos, **un lote después** de cerrarse la ventana | Se descartan: faltan **2.373 eventos**, justo los tardíos del 3 de abril |

```text
window      model                 stream_days  batch_days  …   late  status
2026-04-01  HGST HUH721212ALN604       28,543      28,735  …    576  tardíos descartados
2026-04-01  WDC WUH722222ALE6L4       135,835     136,745  …  2,730  tardíos descartados
2026-04-04  HGST HUH721212ALN604       28,745      28,745  …    576  igual
…
✓ 10 pares iguales; en 5 faltan 2,373 eventos, todos tardíos: el watermark los descartó porque su
  ventana ya estaba cerrada
```

Lo que enseña:

- **La garantía del watermark es en un solo sentido.** Todo evento con menos retraso que el
  umbral se cuenta siempre. Uno que llega más tarde *puede* descartarse, pero no siempre se
  descarta: con 4 días de retraso llegaron en el lote que cerró su ventana, y Spark los sumó
  antes de emitirla. Es lo que dice la documentación de Spark: los datos más tardíos que el
  umbral «pueden o no» agregarse.
- **El descarte depende del tiempo de procesamiento.** Con 5 días de retraso, esos eventos
  cayeron un lote después del cierre. Si los lotes se hubieran repartido de otra forma (por
  ejemplo, con otro trigger), el resultado podría cambiar.
- **Una ventana cerrada no se reabre.** Los 2.373 eventos no aparecen en ninguna salida: la
  ventana 1-3 de abril ya se había emitido en modo `append` y su estado se había borrado. El
  progreso lo registra como «descartadas por el watermark 10». Spark cuenta filas ya
  preagregadas por partición, no eventos sueltos.
- **Elegir el umbral es un compromiso.** Un watermark más largo acepta más retraso, pero cada
  ventana tarda más en publicarse y el estado ocupa más memoria.

## Opciones

En `.env` (las lee Compose) o como argumentos de `docker compose run --rm part-c …`:

| Variable en `.env` | Argumento | Por defecto | Qué cambia |
| ------------------ | --------- | ----------- | ---------- |
| `STREAM_SECONDS_PER_DAY` | `--seconds-per-day` | 10 | Velocidad del reloj acelerado (y del trigger) |
| `STREAM_MODELS` | `--models` | 5 modelos | Lista separada por comas; `all` publica todos (~346.000 eventos por día) |
| `STREAM_SAMPLE_PCT` | `--sample-pct` | 100 | Porcentaje de discos que se publican (muestreo por hash del serial) |
| `STREAM_LATE_PCT` | `--late-pct` | 2 | Porcentaje de discos que llegan tarde (0 = ninguno) |
| `STREAM_LATE_DAYS` | `--late-days` | 1 | Días de retraso de esos discos |
| — | `--window-days` / `--slide-days` | 7 / 7 | Duración y desplazamiento de la ventana (`--slide-days 1` = AFR móvil de 7 días) |
| — | `--watermark-days` | 2 | Umbral del watermark |
| — | `--trigger-seconds` | = segundos por día | Un lote cada N segundos |

Con más días de datos se ven ventanas cerrándose a mitad del flujo, sin esperar a la marca de
fin: con `DS_DAYS=14` hay dos ventanas, y la primera se cierra cuando el watermark llega al 8 de
abril, es decir, tras procesar el día 10.

### Producer en otro terminal

```bash
docker compose run --rm part-c --no-producer   # terminal 1: Spark espera eventos
docker compose run --rm part-c-producer        # terminal 2: publica (arranca después del 1)
```

Así se ve que productor y consumidor están desacoplados por Kafka. `part-c` vacía el topic al
empezar, así que el producer tiene que arrancar después.

### Mirar el topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic drivestats --from-beginning --max-messages 5
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic drivestats   # mensajes por partición
```

```text
{"date":"2026-04-01","serial_number":"7020A0F0F97G","model":"TOSHIBA MG07ACA14TA","failure":0}
…
drivestats:0:211383
drivestats:1:209720
drivestats:2:209086
drivestats:3:210306
```

Con clave = serial, las 4 particiones quedan casi iguales (≈210.000 mensajes cada una). Con
clave = modelo, todo WDC WUH722222ALE6L4 (el 38 % de los eventos) iría a una sola partición:
recibiría al menos el 38 % frente al 25 % de un reparto perfecto, y su tarea marcaría el ritmo
de cada lote. Es el mismo sesgo de la Parte B.

## Salidas (`output/part_c/`)

| Archivo | Contenido |
| ------- | --------- |
| `ventanas.csv` | Resultado definitivo (`append`): `window_start, window_end, model, drive_days, failures, afr_pct, afr_ci95_low, afr_ci95_high, batch_id`. `window_end` es exclusivo |
| `actualizaciones.csv` | Todas las actualizaciones en vivo (`update`), lote a lote |
| `progreso.csv` | Por lote y consulta: eventos leídos, watermark, filas en el estado, filas descartadas y desglose de tiempos (`durationMs`) |
| `verificacion.csv` | Streaming frente a batch por (ventana, modelo) y su estado |
| `productor.json` | Lo que publicó el producer: eventos por fecha y eventos tardíos por fecha y modelo |
| `resumen.json` | Parámetros, eventos leídos, descartes y resultado de la verificación |
| `checkpoints/` | Offsets y estado de cada consulta; se borra al empezar cada ejecución |

## Decisiones de diseño

- **Un solo comando.** El driver lanza el producer como subproceso y reenvía su salida, así que
  `docker compose run --rm part-c` muestra todo en orden y termina solo. El producer también
  puede ir aparte (`part-c-producer`).
- **Topic y checkpoints nuevos en cada ejecución.** Si se reutilizara el topic, Spark leería los
  datos de la ejecución anterior desde `earliest` y contaría todo dos veces.
- **La marca de fin no se filtra en el plan.** Spark empuja los filtros que no usan la columna
  de tiempo por debajo del watermark (*predicate pushdown*), así que filtrar `__fin__` antes de
  agregar haría que la marca no moviera el watermark. Se descarta al leer los resultados.
- **El producer en Python aguanta gracias a tres detalles.** Corta solo las 5 primeras columnas
  con `split` en vez de parsear las ~190 con `csv`. Codifica el JSON con una plantilla y llama a
  `poll()` cada 1.000 mensajes. Así lee y publica un día en 2-5 s con Spark corriendo al lado,
  frente a los 16-20 s de la primera versión.
