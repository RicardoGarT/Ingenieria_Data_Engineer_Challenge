# GFB · Data Engineer Challenge

Solución a los dos ejercicios (más el **Plus de Ecobici**) del Data Engineer Challenge de Banco BASE / GFB.

---

## Tabla de contenidos

- [Estructura del repositorio]
- [Ejercicio 1 — ETL Airflow + MinIO + Trino]
- [Ejercicio 2 — Diseño de arquitectura]
- [Plus — DAG Ecobici en tiempo real]
- [Cómo ejecutar (resumen)]
- [Stack utilizado]

---

## Estructura

```
.
├── ejercicio1/                     # Pipeline ETL Airflow + MinIO + Trino (Polars)
│   ├── Airflow/dags/
│   │   ├── etl_engineer_challenge.py
│   │   └── ecobici_station_status_etl.py     # Plus
│   ├── Hive/metastore-site.xml
│   ├── Trino/etc/...
│   ├── data_prueba_tecnica.csv
│   ├── docker-compose.yaml
│   └── README.md                  # Detalle del pipeline + respuestas a las preguntas
├── ejercicio2/                     # Diseño de arquitectura (sólo propuesta)
│   ├── diagrama.svg
│   └── justificacion_de_tu_arquitectura.pdf
└── README.md                       # (este archivo)
```

---

## Ejercicio 1 — Resumen

Pipeline ETL que ingesta `data_prueba_tecnica.csv` (10,000 transacciones), lo limpia con **Polars** (Plus del reto), lo materializa como Parquet en MinIO y lo expone en **Trino** vía una tabla externa Hive `bronze.prueba.tbl_data`.

### Datos clave del *profiling*

| Hallazgo | Detalle | Tratamiento |
|---|---|---|
| `id` nulo | **3** filas | Descartadas (no imputable, sin trazabilidad) |
| `name` corrupto/vacío | **5** filas | Imputación cruzada usando pares íntegros como diccionario |
| `company_id` corrupto/vacío | **5** filas | Imputación cruzada usando pares íntegros como diccionario |
| `amount` con valores absurdos | **5** valores (`inf`, `3e+34`, `2.13e+18`, …) | → `NULL` |
| `status` corrupto | **2** valores (`p&0x3fid`, `0xFFFF`) | → `unknown` |
| `created_at` con formato no estándar | **3** filas (`YYYYMMDD`, ISO con `T`) | Parser multi-formato |

### Resultado final

- **9,997 filas limpias** en el master.
- **2 marcas** identificadas: `MiPasajefy`, `Muebles chidos`.
- **9 estatus**: 8 válidos + `unknown`.
- **Agregaciones por `(name, created_at)`** embebidas como columnas adicionales, listas para consulta directa en Trino sin necesidad de `GROUP BY` en cada query.

> 📄 Detalles, decisiones y respuestas a las **preguntas adicionales** del reto: ver [`ejercicio1/README.md`](ejercicio1/README.md).

---

## Ejercicio 2 — Resumen

Arquitectura **Lakehouse Iceberg** sobre object storage, alimentada por:

| Fuente | Mecanismo | Herramienta |
|---|---|---|
| F2 (SQL Server) | CDC | **Debezium + Kafka** |
| F3 (PostgreSQL) | CDC | **Debezium + Kafka** |
| F1 (CRM propietario) | Extracción incremental | **Airbyte** |

**Capas y motores:**

- **Capas medallion** (Bronze → Silver → Gold) con **Spark / dbt-trino** como motor de transformación.
- **Trino** para consumo SQL operativo.
- **Spark / Polars + Neo4j** para ciencia de datos (clustering / grafos).

**Cobertura de preguntas del reto:**

- **I.A–I**: subconjuntos, retos, mitigación de impacto, etapas, herramientas, storage, orquestación, diagrama.
- **II.A**: seguridad end-to-end.
- **III.A**: gobernanza y metadata.

> Entregables: [`ejercicio2/diagrama.svg`](ejercicio2/diagrama.svg) y [`ejercicio2/justificacion_de_tu_arquitectura.pdf`](ejercicio2/justificacion_de_tu_arquitectura.pdf).

---

## Plus — DAG Ecobici

DAG `ecobici_station_status_etl` que ingesta el feed GBFS `station_status` de Ecobici CDMX **cada 10 min**, limpia con Polars y mantiene **un único parquet por día** en:

```
bck-bronze/ecobici/station_status/dt=YYYY-MM-DD/data.parquet
```

(modo append por deduplicación). La tabla `bronze.ecobici.station_status` queda particionada por `dt`.

> Detalle y respuestas en [`ejercicio1/README.md` — sección Plus](ejercicio1/README.md#plus--dag-ecobici-ecobici_station_status_etl).

---

## Cómo ejecutar (resumen)

```bash
cd ejercicio1
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d
```

**Esperar a que `airflow-apiserver` esté healthy y entrar a la UI:**

| Servicio | URL / Endpoint | Credenciales |
|---|---|---|
| Airflow  | http://localhost:8080 | `airflow` / `airflow` |
| MinIO    | http://localhost:9001 | `minio` / `minio1234` |
| Trino    | `jdbc:trino://localhost:8090` | `root` (sin password) |

```bash
docker compose exec airflow-scheduler airflow dags unpause etl_engineer_challenge
docker compose exec airflow-scheduler airflow dags trigger etl_engineer_challenge
```

> Para más detalle (verificación, troubleshooting y captura DBeaver) ver [`ejercicio1/README.md`](ejercicio1/README.md).

---

## Stack utilizado

| Componente | Versión / Detalle |
|---|---|
| **Apache Airflow** | 3.2 (CeleryExecutor + Postgres + Redis) |
| **MinIO** | Object storage S3-compatible |
| **Trino** | 467 |
| **Hive Metastore** | 3 |
| **MySQL** | 8 (catálogo del Metastore) |
| **Polars** | 1.x — procesamiento (Plus del reto) |
| **boto3** | Operaciones de control plane S3 (creación de buckets, upload) |