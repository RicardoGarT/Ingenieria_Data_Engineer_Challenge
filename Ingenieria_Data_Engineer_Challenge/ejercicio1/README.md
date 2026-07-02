## Tabla de contenidos

- [Resumen]
- [Arquitectura]
- [Stack de servicios]
- [Cómo ejecutar]
- [DAG principal: `etl_engineer_challenge`]
- [Detalle técnico de `transform_to_bronze`]
- [Conexión desde DBeaver]
- [Análisis del dataset y respuestas a las preguntas]
- [Mejoras propuestas para siguientes versiones]
- [Evidencia (Trino + DBeaver)]
- [Plus: DAG Ecobici en tiempo real]
- [Estructura del repositorio]
- [Decisiones técnicas relevantes]

---

## Resumen

Este proyecto implementa un pipeline ETL orquestado con **Apache Airflow 3.2.0** (TaskFlow API) que:

1. **Ingesta** `data_prueba_tecnica.csv` (10,000 transacciones) hacia una capa *landing* en **MinIO**.
2. **Limpia, normaliza y agrega** los datos con **Polars**, aplicando reglas de calidad de datos derivadas de un *profiling* exhaustivo del CSV fuente.
3. **Materializa** el resultado como Parquet en una capa *bronze*.
4. **Expone** el Parquet como tabla SQL consultable vía **Trino**, lista para conectarse desde DBeaver u otra herramienta BI.

Como *plus*, se incluye un segundo DAG (`ecobici_station_status_etl`) que ingesta en near-real-time el feed público de disponibilidad de bicicletas de Ecobici CDMX, demostrando un patrón de append incremental con deduplicación sobre un único archivo Parquet por día.

---

## Arquitectura

```
                    ┌──────────────────────────┐
                    │  data_prueba_tecnica.csv  │
                    │     (10,000 filas)        │
                    └─────────────┬──────────────┘
                                  │  ingest_to_landing
                                  ▼
        ┌────────────────────────────────────────────────┐
        │  MinIO · bck-landing/data/                      │
        │  data_prueba_tecnica.csv                        │
        └─────────────────────┬────────────────────────────┘
                               │  transform_to_bronze (Polars)
                               │  · normalización de strings
                               │  · validación de IDs (regex SHA-1)
                               │  · imputación cruzada name ↔ company_id
                               │  · normalización de status y amount
                               │  · parseo multi-formato de fechas
                               │  · agregaciones por (name, created_at)
                               ▼
        ┌────────────────────────────────────────────────┐
        │  MinIO · bck-bronze/master/                     │
        │  data_prueba_tecnica.parquet                    │
        └─────────────────────┬────────────────────────────┘
                               │  expose_in_trino
                               ▼
        ┌────────────────────────────────────────────────┐
        │  Trino · bronze.prueba.tbl_data                 │
        │  (tabla externa sobre el Parquet)                │
        └─────────────────────┬────────────────────────────┘
                               │
                               ▼
                         DBeaver / SQL client
```

**Principio de diseño:** cada capa (landing → bronze → SQL) es un contrato de datos independiente. Airflow solo orquesta el movimiento y transformación; MinIO es el storage; Trino es exclusivamente el motor de consulta — nunca hay lógica de negocio en la capa SQL.

---

## Stack de servicios
## Servicios (docker-compose)

| Servicio        | Puerto host  | Notas                                             |
|-----------------|--------------|---------------------------------------------------|
| Airflow API     | `8080`       | UI: http://localhost:8080  (user/pass: `airflow`) |
| MinIO           | `9000/9001`  | Console: http://localhost:9001 (`minio/minio1234`)|
| Trino           | `8090`       | JDBC: `jdbc:trino://localhost:8090` user `root`   |
| Hive Metastore  | `9083`       | Backend MySQL                                     |
| MySQL           | `3306`       | Metastore DB                                      |

> El CSV fuente se monta como volumen de solo lectura dentro del contenedor de Airflow:
> `./data_prueba_tecnica.csv → /opt/airflow/data/data_prueba_tecnica.csv`

---

## Cómo ejecutar

```bash
cd ejercicio1

# 1) Levantar el stack completo
docker compose up -d

# 2) Esperar a que Airflow reporte healthy (~1-2 min la primera vez)
docker compose ps
#    Verificar visualmente en http://localhost:8080

# 3) Despausar y disparar el DAG principal
# Opción A — desde la UI:
#   activar el toggle de 'etl_engineer_challenge' → botón "Trigger DAG"

# Opción B — por CLI:
docker compose exec airflow-scheduler airflow dags unpause etl_engineer_challenge
docker compose exec airflow-scheduler airflow dags trigger etl_engineer_challenge

# 4) Seguir el progreso
docker compose exec airflow-scheduler airflow dags list-runs -d etl_engineer_challenge
```

### Verificación rápida (opcional)

```bash
# Ver el log de la tarea de transformación (incluye resumen de filas y top agregaciones)
docker compose exec airflow-scheduler airflow tasks logs \
  etl_engineer_challenge transform_to_bronze <execution_date>

# Confirmar que la tabla existe en Trino
docker compose exec trino trino --execute "SHOW TABLES FROM bronze.prueba;"
```

### Apagar el stack

```bash
docker compose down          # detiene contenedores, conserva volúmenes
docker compose down -v       # además borra volúmenes (reinicio limpio)
```

---

## DAG principal: `etl_engineer_challenge`

| Orden | Tarea                  | Responsabilidad                                                                 |
|:-----:|-------------------------|-------------------------------------------------------------------------------------|
| 1     | `setup_buckets`         | Crea `bck-landing` y `bck-bronze` en MinIO si no existen (idempotente vía `head_bucket`). |
| 2     | `ingest_to_landing`     | Sube el CSV local a `bck-landing/data/data_prueba_tecnica.csv`.                       |
| 3     | `transform_to_bronze`   | Lee con Polars, limpia, valida, imputa, agrega y escribe el Parquet en bronze.         |
| 4     | `expose_in_trino`       | Crea el schema `bronze.prueba` y la tabla externa `tbl_data` sobre el Parquet.         |

**Dependencia lineal:**

```
setup_buckets ──▶ ingest_to_landing ──▶ transform_to_bronze ──▶ expose_in_trino
```

Se eligió una dependencia estrictamente lineal (en vez de paralelizar tareas) porque cada tarea consume la salida directa de la anterior — no hay ramas independientes que se beneficien de ejecución concurrente en este pipeline.

**Configuración del DAG:**

| Parámetro | Valor | Motivo |
|---|---|---|
| `schedule` | `None` | Ejecución manual/on-demand, apropiada para un challenge donde se controla el disparo. |
| `catchup` | `False` | Evita backfills no deseados si `start_date` queda en el pasado. |
| `retries` | `1` | Un reintento automático ante fallos transitorios (red, timeouts de MinIO/Trino). |
| `retry_delay` | `3 min` | Ventana de espera razonable para que el servicio afectado se recupere. |

---

## Detalle técnico de `transform_to_bronze`

Esta es la tarea con más lógica del pipeline. Su flujo interno, en orden de ejecución:

```
1. Lectura del CSV desde MinIO
   → infer_schema_length=0 (todo como Utf8) para controlar el casteo manualmente
   → normaliza saltos de línea \r\n → \n y nombres de columnas (strip)

2. Limpieza de strings
   → strip_chars() sobre id, name, company_id, status

3. Validación de identificadores
   → id y company_id deben cumplir regex ^[0-9a-fA-F]{40}$ (formato SHA-1)
   → lo que no cumple → null

4. Detección de corrupción en `name`
   → si contiene el marcador "0x" → null

5. Imputación cruzada name ↔ company_id
   → construye diccionarios {company_id: name} y {name: company_id}
     a partir de los pares íntegros observados en el propio dataset
   → rellena el campo faltante cuando el otro sigue siendo válido

6. Normalización de `status`
   → valores fuera de VALID_STATUSES (8 categorías) → "unknown"

7. Validación de `amount`
   → cast a Float64 (strict=False)
   → rechaza (→ null) valores no finitos o >= 1e9

8. Parseo multi-formato de fechas (created_at, paid_at)
   → coalesce de 3 formatos: %Y-%m-%d, %Y%m%d, ISO con "T"

9. Regla de negocio sobre paid_at
   → null si status ∈ {voided, pending_payment, expired, pre_authorized}
   → null si paid_at < created_at

10. Filtro final
    → descarta filas con id nulo (no imputable, ver sección de análisis)

11. Agregaciones por (name, created_at)
    → tx_count_by_name_day, amount_sum_by_name_day,
      amount_avg_by_name_day, paid_count_by_name_day
    → se unen (left join) de vuelta al detalle para preservar granularidad
      de transacción individual (no se pierde ninguna fila del detalle)

12. Escritura del Parquet
    → compresión snappy, subido a MinIO vía boto3 (put_object)
    → una única ruta canónica: bck-bronze/master/data_prueba_tecnica.parquet
```

---

## Conexión desde DBeaver

| Parámetro    | Valor                              |
|--------------|-------------------------------------|
| Driver       | **Trino**                          |
| URL JDBC     | `jdbc:trino://localhost:8090`      |
| Usuario      | `root` (sin password)              |
| Catálogo     | `bronze`                           |
| Schema       | `prueba`                           |
| Tabla        | `tbl_data`                         |

**Consultas de ejemplo:**

```sql
-- Top 10 días por monto cobrado
SELECT 
  name,
  created_at,
  amount_sum_by_name_day,
  paid_count_by_name_day
FROM bronze.prueba.tbl_data
GROUP BY 
  name,
  created_at,
  amount_sum_by_name_day,
  paid_count_by_name_day
ORDER BY amount_sum_by_name_day DESC
LIMIT 10;



-- Distribución de status
SELECT 
  status, 
  count(*) AS n
FROM bronze.prueba.tbl_data
GROUP BY status
ORDER BY n DESC;
```

```sql
-- Chequeo de calidad: filas sin amount válido (outliers descartados)
SELECT 
  count(*) AS filas_amount_null
FROM bronze.prueba.tbl_data
WHERE amount IS NULL;

-- Chequeo de integridad: paid_at nunca antes que created_at
SELECT 
  count(*) AS violaciones
FROM bronze.prueba.tbl_data
WHERE paid_at < created_at;
```

---

## 🧪 Análisis del dataset y respuestas a las preguntas

Antes de implementar la limpieza se hizo un **profiling exhaustivo** del CSV. Los hallazgos y decisiones que siguen están respaldados por conteos exactos sobre las 10,000 filas.

###  `id` nulos — ¿qué hacer con ellos?

| Hallazgo | Detalle |
|---|---|
| Filas con `id` vacío | **3** |
| Formato del resto | Cumple SHA-1 (40 chars hex) |
| Duplicados | Ninguno |

**Decisión: descartarlas** del dataset master.

**Justificación:**
- `id` es la clave natural de la transacción; sin ella no hay trazabilidad, no se puede deduplicar, ni reconciliar contra la fuente.
- **No es imputable**: no existe otra combinación de campos (`name + company_id + amount + created_at`) que sea única — esas combinaciones se repiten legítimamente en el dataset.
- El impacto es despreciable: `3/10,000 = 0.03%`.

**Mitigación recomendada a futuro:** en vez de descartar en silencio, generar un área de *quarantine* (`bck-landing/quarantine/`) con las filas rechazadas y disparar una alerta (Slack/email) al área operativa. En la versión actual del pipeline, el `print` final ya reporta cuántas filas se descartaron y por qué.

###  `name` y `company_id` — inconsistencias y mitigación

| Patrón                                | Filas | Ejemplo                                |
|----------------------------------------|:-----:|------------------------------------------|
| `name` vacío                          | 3     | `''`                                     |
| `name` con marcador corrupto `0x...`  | 2     | `MiPas0xFFFF`, `MiP0xFFFF`               |
| `company_id` vacío                    | 4     | `''`                                     |
| `company_id` corrupto (no SHA-1)      | 1     | `*******`                                |
| `name` con >1 `company_id` asociado   | 1     | `MiPasajefy` con un `company_id` roto    |

**Contexto clave:** en la fuente solo existen **2 entidades reales** — `MiPasajefy` y `Muebles chidos` — cada una con su `company_id` SHA-1. Los valores `0x...` corresponden a corrupción de bytes (la misma firma aparece también en `status` y en `amount`); el valor `*******` parece un enmascaramiento aplicado incorrectamente en origen.

**Pipeline de mitigación implementado** (ver también la sección de [detalle técnico](#-detalle-técnico-de-transform_to_bronze)):

```
1. Normalización de strings          → strip_chars() sobre id, name, company_id, status
2. Validación de formato             → regex ^[0-9a-fA-F]{40}$ en id y company_id; inválido → null
3. Detección de marcador "0x"        → si name lo contiene → null
4. Imputación cruzada name↔company_id → usando pares íntegros observados como diccionario
5. Resultado final                   → name y company_id quedan con solo 2 valores válidos cada uno
```

El paso 4 (imputación cruzada) es el más relevante: construye un diccionario `company_id → name` y otro `name → company_id` a partir de los pares ya íntegros en el propio dataset, y lo usa para rellenar el campo faltante cuando el otro sigue siendo válido. Esto recupera las filas donde solo uno de los dos campos estaba corrupto.

**Mitigación recomendada a futuro:** el mapeo `name ↔ company_id` no debería inferirse del archivo transaccional en cada corrida — debería vivir en una **dimensión maestra** (p. ej. `bck-bronze/dim/dim_company.parquet`) poblada desde el CRM/ERP fuente de verdad.

###  Resto de campos — valores atípicos y procedimiento

#### `amount`

Sin nulos en el CSV original, pero con corrupción severa en la magnitud:

| Estadístico | Valor |
|---|---|
| Valores absurdos detectados | 5 (`21,312,312,393.19`; `2.13e16`; `2.13e18`; `3.0e34`; `inf`) |
| p99 | 5,148.21 |
| p99.9 | 42,243.0 |
| Máximo legítimo observado | 83,983.0 |
| Negativos / ceros | Ninguno |

> Los 5 outliers absurdos ocurren únicamente en transacciones `pending_payment` o `voided` — no afectan el revenue real reportado como `paid`.

**Decisión:** castear a `Float64` y rechazar (→ `null`) cualquier valor no finito o `>= 1e9`. El umbral `1e9` es holgado frente al p99.9, pero corta limpiamente los 5 outliers absurdos sin arriesgar falsos positivos sobre transacciones legítimas de alto valor. Las 5 filas afectadas **se conservan** en el master (con `amount = null`), en vez de descartarse por completo — preservan `id` y el resto de campos válidos.

#### `status`

10 categorías observadas en total:

- **8 válidas:** `paid`, `voided`, `pending_payment`, `refunded`, `charged_back`, `pre_authorized`, `expired`, `partially_refunded`.
- **2 corruptas** (1 fila cada una): `p&0x3fid`, `0xFFFF`.

**Decisión:** mapear las corruptas a `unknown`. Se decidió **no inferir** (aunque `p&0x3fid` visualmente parezca `paid`) para no alterar la verdad operativa sin evidencia — se flaggean explícitamente para que el área de negocio decida.

#### `created_at`

3 filas con formato no estándar:

| Formato encontrado | Ejemplo |
|---|---|
| ISO con separador `T` | `2019-02-27T00:00:00` |
| Compacto sin separadores | `20190516`, `20190121` |

**Decisión:** parser multi-formato vía `coalesce()` de tres `strptime` (`%Y-%m-%d`, `%Y%m%d`, ISO con `T`). Resultado: `created_at` queda 100% no nulo y dentro del rango `[2019-01-01, 2019-05-20]`.

#### `paid_at`

3,991 valores nulos (~40% del dataset).

- **Verificación:** los nulos corresponden 1:1 con `status ∈ {voided, pending_payment, expired, pre_authorized}` — es el comportamiento esperado del negocio, **no** una inconsistencia de datos.
- **Regla de negocio adicional:** si `paid_at < created_at`, se anula `paid_at`. No se observó ningún caso en este dataset, pero la lógica queda implementada en el DAG como salvaguarda para cargas futuras.

**Chequeos de integridad adicionales realizados:**
- `id` es único en todas las filas válidas (sin duplicados).
- `paid_at` nunca antecede a `created_at` en el resultado final.

---

## Mejoras al ETL para próximas versiones

| # | Mejora | Beneficio |
|:-:|---|---|
| 1 | Migrar a patrón **Bronze → Silver → Gold** (medallion architecture). Hoy `master` mezcla limpieza + agregaciones. | Trazabilidad clara: Bronze = copia fiel tipada, Silver = limpio, Gold = agregados. |
| 2 | **Escritura particionada** (`year=YYYY/month=MM/day=DD`) con **Iceberg/Delta** en vez de Parquet plano. | Cargas incrementales, time-travel, schema evolution, vacuum. |
| 3 | **Cargas incrementales** vía `data_interval_start/end` o sensor de objeto en MinIO, en vez de full refresh. | Menor costo, menor latencia, sin overwrite total. |
| 4 | **Validación con Great Expectations / Soda / pydeequ** como tarea previa al sink. | Frena datos malos antes de que contaminen Bronze. |
| 5 | **Quarantine bucket** (`bck-quarantine/{batch_id}/...`) + métricas de DQ a Prometheus/StatsD. | Visibilidad de qué se descartó y por qué. |
| 6 | **Catálogo maestro `dim_company`** poblado desde CRM/ERP, no inferido del transaccional. | Imputación robusta y reproducible. |
| 7 | **Airflow Connections** (`aws_default`, `trino_default`) en vez de credenciales hardcodeadas. | Higiene de secretos. |
| 8 | **Tests unitarios** sobre las funciones de limpieza (pytest + DataFrames sintéticos). | Confiabilidad ante cambios de schema fuente. |
| 9 | **Sensor de archivo nuevo** en `bck-landing/data/` (`S3KeySensor`). | Disparo basado en evento, no manual. |
| 10 | **Lineage / OpenLineage** en Airflow. | Trazabilidad columna ↔ origen; gobernanza de datos. |
| 11 | **Idempotencia explícita** vía `MERGE` Iceberg sobre `id`, en vez de `DROP/CREATE` Hive. | Reprocesos seguros sin duplicar ni perder datos. |
| 12 | **Métricas de drift** del catálogo `name`/`status` (alerta si aparece un valor nuevo). | Detección temprana de cambios upstream. |

---



## Plus — DAG Ecobici (`ecobici_station_status_etl`)

Ingesta el feed **GBFS** [`station_status`](https://gbfs.mex.lyftbikes.com/gbfs/en/station_status.json) de Ecobici CDMX (operado por Lyft) **cada 10 minutos**, lo limpia con Polars, y mantiene **un único Parquet por día** en:

```
bck-bronze/ecobici/station_status/dt=YYYY-MM-DD/data.parquet
```

La tabla Trino expone los datos particionados por fecha.

### Tareas del DAG

| Tarea                    | Acción                                                                                       |
|----------------------------|-------------------------------------------------------------------------------------------------|
| `ensure_bronze_setup`      | Crea bucket + schema + tabla externa Hive particionada (idempotente).                          |
| `fetch_station_status`     | Descarga el JSON del feed GBFS.                                                                 |
| `clean_and_append`         | Lee el Parquet del día (si existe), une el snapshot nuevo, deduplica por `(station_id, last_reported)` y reescribe **un solo** Parquet. |
| `sync_trino_partitions`    | Ejecuta `CALL system.sync_partition_metadata` para que Trino descubra la partición del día.     |

### Respuestas a las preguntas del documento adicional

**¿Qué dataset se seleccionó?**

GBFS `station_status` del sistema Ecobici CDMX: para cada una de las ~677 estaciones activas devuelve bicis y docks disponibles/inhabilitados, banderas operativas (`is_installed`, `is_renting`, `is_returning`, `is_charging`) y `last_reported`.

**¿Con qué temporalidad se realiza la extracción y por qué?**

**Cada 10 minutos.** Razones:

- El feed declara `ttl=10s` y se actualiza con altísima frecuencia — podría extraerse cada minuto, pero **10 min es un buen trade-off**: ~144 snapshots/día, suficiente para análisis de ocupación hora-pico vs. valle, sin saturar la API ni inflar el lake.
- Una estación cambia de estado "de verdad" cada vez que se toma o devuelve una bici; en la práctica, un Δ de 10 min captura la mayoría de los cambios relevantes. La deduplicación por `(station_id, last_reported)` evita persistir lecturas idénticas si la estación no cambió.
- No hay autenticación ni rate-limit conocido documentado; aun así, 10 min es una cadencia conservadora y respetuosa con el proveedor.

**¿Qué limpieza de datos se aplicó?**

```
1. Cast canónico de tipos     → station_id: varchar · counts: int32 · is_*: boolean (feed emite 1/0)
                                 last_reported: epoch segundos → timestamp
2. Filtros de validez         → descarta station_id/last_reported nulos, o counts negativos (corrupción)
3. Metadatos de trazabilidad  → snapshot_ts (momento de ingesta) + dt (clave de partición YYYY-MM-DD)
4. Deduplicación              → por (station_id, last_reported) al hacer el append
```

**¿Qué esquema de partición se eligió y cómo afecta a la disponibilización en Trino?**

Partición diaria: `bck-bronze/ecobici/station_status/dt=YYYY-MM-DD/data.parquet`.

**Sí, afecta directamente:**

- Con el **conector Hive** (el configurado en este proyecto), Trino **no** descubre particiones nuevas automáticamente — una carpeta `dt=2026-04-29/` recién creada permanece invisible hasta registrarse en el metastore.
- Por eso el DAG cierra su corrida con `CALL system.sync_partition_metadata(schema, table, mode => 'ADD')`, que sincroniza contra S3 las particiones existentes que el metastore aún no conoce.
- Con **Iceberg** en su lugar, este paso de sync no sería necesario: Iceberg mantiene su propio manifiesto de archivos en el catálogo y resuelve particiones vía *hidden partitioning*. Sería la recomendación para producción.

**¿Cuál fue el reto más grande de este proceso y por qué?**

Conciliar dos requisitos en tensión:

1. **Append continuo** — llegan datos nuevos cada 10 minutos.
2. **Un único Parquet por partición** — sin acumular archivos pequeños.

Parquet no soporta append nativo, así que la solución trata el archivo del día como una *tabla pequeña en memoria*: se lee con Polars, se concatena el snapshot nuevo, se deduplica por `(station_id, last_reported)` y se reescribe completo. Esto mantiene un solo archivo por partición y garantiza idempotencia (correr el mismo snapshot dos veces no duplica filas), pero introduce puntos a vigilar:

- **Race conditions** ante corridas concurrentes → mitigado con `max_active_runs=1`.
- **Latencia creciente** dentro de un mismo día: el Parquet crece con cada corrida y la rescritura se vuelve más costosa conforme avanza el día. A escala, la solución correcta sería migrar a Iceberg con `MERGE` (más compactación periódica), o particionar también por hora para acotar el tamaño del archivo reescrito.
- **Consistencia eventual** de S3/MinIO: no representa un problema con un solo objeto y un solo escritor, pero sí requeriría atención con múltiples escritores concurrentes.

---

## Estructura del repo (este ejercicio)

```
ejercicio1/
├── Airflow/
│   ├── config/
│   │   └── airflow.cfg
│   └── dags/
│       ├── etl_engineer_challenge.py
│       └── ecobici_station_status_etl.py     # Plus
├── Hive/
│   └── metastore-site.xml
├── Trino/
│   └── etc/
│       ├── catalog/
│       │   └── bronze.properties
│       └── config.properties
├── data_prueba_tecnica.csv
├── docker-compose.yaml
└── README.md
```

---

##  Decisiones técnicas relevantes

- **Polars** (sección Plus del reto) para todo el cómputo: expresiones vectorizadas, *streaming-friendly*, y con soporte *lazy* si el dataset crece más allá de lo que cabe cómodamente en memoria.
- **Tabla externa Hive sobre Parquet** (no managed): el DAG controla explícitamente el ciclo de vida del archivo; Trino solo lo expone para consulta, sin duplicar responsabilidades.
- **Un único Parquet** en `bck-bronze/master/`, para que la tabla externa apunte a la carpeta sin ambigüedad de múltiples archivos ni sufijos automáticos de partición.
- **`infer_schema_length=0`** (todo se lee como `Utf8`) para decidir explícitamente las reglas de casteo por columna, en vez de dejar que Polars infiera y rechace el CSV completo por un solo valor `inf` en `amount`.
- **Escritura vía boto3 (`put_object`)** en vez de un writer nativo Hive-partitioned, para mantener una única ruta canónica sin sufijos automáticos de partición que complicarían el `external_location` de la tabla en Trino.