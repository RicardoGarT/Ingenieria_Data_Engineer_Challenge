"""
DAG: etl_engineer_challenge

Pipeline ETL de transacciones (landing -> bronze -> Trino).

Etapas:
    1. setup_buckets
       Crea los buckets 'bck-landing' y 'bck-bronze' en MinIO.
       Operación idempotente (no falla si ya existen).

    2. ingest_to_landing
       Sube 'data_prueba_tecnica.csv' a 'bck-landing/data/'.

    3. transform_to_bronze
       Lee el CSV con Polars, limpia y agrega los datos, y escribe
       el resultado en 'bck-bronze/master/data_prueba_tecnica.parquet'.

    4. expose_in_trino
       Crea el schema 'bronze.prueba' y la tabla 'tbl_data' apuntando
       al parquet generado.

Notas de diseño:
    - La capa de procesamiento usa Polars (sección Plus del reto).
    - El parquet se materializa en el worker de Airflow y se sube a
      MinIO vía boto3, en lugar de escribirse directo con un writer
      Hive-partitioned, para mantener una única ruta canónica sin
      sufijos automáticos.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from airflow.decorators import dag, task

# --- MinIO: conexión ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio1234"

# --- MinIO: buckets ---
BUCKET_LANDING = "bck-landing"
BUCKET_BRONZE = "bck-bronze"

# --- Paths / keys de los archivos ---
CSV_SOURCE_PATH = "/opt/airflow/data/data_prueba_tecnica.csv"
LANDING_CSV_KEY = "data/data_prueba_tecnica.csv"
BRONZE_PARQUET_KEY = "master/data_prueba_tecnica.parquet"

# --- Trino: conexión ---
TRINO_HOST = "coordinator"
TRINO_PORT = 8080
TRINO_USER = "root"

# --- Trino: ubicación de la tabla ---
TRINO_CATALOG = "bronze"
TRINO_SCHEMA = "prueba"
TRINO_TABLE = "tbl_data"

VALID_STATUSES = {
    "paid",
    "voided",
    "pending_payment",
    "refunded",
    "charged_back",
    "pre_authorized",
    "expired",
    "partially_refunded",
}

def _s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url = MINIO_ENDPOINT,
        aws_access_key_id = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
        region_name = "us-east-1",
    )

@dag(
    dag_id = "etl_engineer_challenge",
    description = "ETL de transacciones: landing -> bronze (Polars) -> Trino",
    start_date = datetime(2024, 1, 1),
    schedule = None,
    catchup = False,
    default_args = {
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes = 3),
    },
    tags = ["challenge", "polars", "minio", "trino"],
)

def etl_engineer_challenge():
    @task
    def setup_buckets() -> dict:
        from botocore.exceptions import ClientError

        s3 = _s3_client()
        created = []
        for bucket in (BUCKET_LANDING, BUCKET_BRONZE):
            try:
                s3.head_bucket(Bucket = bucket)
            except ClientError:
                s3.create_bucket(Bucket = bucket)
                created.append(bucket)
        return {"created": created, "ensured": [BUCKET_LANDING, BUCKET_BRONZE]}

    @task
    def ingest_to_landing() -> str:
        local = Path(CSV_SOURCE_PATH)
        if not local.exists():
            raise FileNotFoundError(
                f"CSV no encontrado en {CSV_SOURCE_PATH}. "
                "Verifica el volumen montado en docker-compose."
            )
        s3 = _s3_client()
        s3.upload_file(str(local), BUCKET_LANDING, LANDING_CSV_KEY)
        return f"s3://{BUCKET_LANDING}/{LANDING_CSV_KEY}"

    @task
    def transform_to_bronze() -> str:
        import polars as pl

        s3 = _s3_client()
        obj = s3.get_object(Bucket = BUCKET_LANDING, Key = LANDING_CSV_KEY)
        raw_bytes = obj["Body"].read()

        df = pl.read_csv(
            BytesIO(raw_bytes.replace(b"\r\n", b"\n")),
            infer_schema_length = 0,
            null_values = ["", "NULL", "null", "NaN", "nan"],
        )
        df = df.rename({c: c.strip() for c in df.columns})
        n_in = df.height

        df = df.with_columns(
            [
                pl.col(c).str.strip_chars().alias(c)
                for c in ("id", "name", "company_id", "status")
            ]
        )

        df = df.with_columns(
            pl.when(pl.col("id").str.contains(r"^[0-9a-fA-F]{40}$"))
            .then(pl.col("id"))
            .otherwise(None)
            .alias("id")
        )

        df = df.with_columns(
            pl.when(pl.col("name").str.contains("0x", literal=True))
            .then(None)
            .otherwise(pl.col("name"))
            .alias("name")
        )

        df = df.with_columns(
            pl.when(pl.col("company_id").str.contains(r"^[0-9a-fA-F]{40}$"))
            .then(pl.col("company_id"))
            .otherwise(None)
            .alias("company_id")
        )

        pairs = (
            df.filter(pl.col("name").is_not_null() & pl.col("company_id").is_not_null())
            .group_by(["name", "company_id"])
            .agg(pl.len().alias("freq"))
            .sort("freq", descending = True)
        )
        cid_to_name = {
            row["company_id"]: row["name"]
            for row in pairs.unique(subset = ["company_id"], keep = "first").to_dicts()
        }
        name_to_cid = {
            row["name"]: row["company_id"]
            for row in pairs.unique(subset = ["name"], keep = "first").to_dicts()
        }

        df = df.with_columns(
            pl.when(pl.col("name").is_null() & pl.col("company_id").is_not_null())
            .then(pl.col("company_id").replace_strict(cid_to_name, default = None))
            .otherwise(pl.col("name"))
            .alias("name"),
        )
        df = df.with_columns(
            pl.when(pl.col("company_id").is_null() & pl.col("name").is_not_null())
            .then(pl.col("name").replace_strict(name_to_cid, default = None))
            .otherwise(pl.col("company_id"))
            .alias("company_id"),
        )

        df = df.with_columns(
            pl.when(pl.col("status").is_in(list(VALID_STATUSES)))
            .then(pl.col("status"))
            .otherwise(pl.lit("unknown"))
            .alias("status")
        )

        amount_parsed = pl.col("amount").cast(pl.Float64, strict = False)
        df = df.with_columns(
            pl.when(amount_parsed.is_finite() & (amount_parsed >= 0) & (amount_parsed < 1e9))
            .then(amount_parsed)
            .otherwise(None)
            .alias("amount")
        )

        def parse_dates(col: str) -> pl.Expr:
            base = pl.col(col)
            ymd = base.str.strptime(pl.Date, "%Y-%m-%d", strict = False)
            compact = base.str.strptime(pl.Date, "%Y%m%d", strict = False)
            iso = (
                base.str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict = False)
                .dt.date()
            )
            return pl.coalesce([ymd, compact, iso]).alias(col)

        df = df.with_columns(parse_dates("created_at"), parse_dates("paid_at"))

        unpaid_statuses = ["voided", "pending_payment", "expired", "pre_authorized"]
        df = df.with_columns(
            pl.when(pl.col("status").is_in(unpaid_statuses))
            .then(None)
            .when(
                pl.col("paid_at").is_not_null()
                & pl.col("created_at").is_not_null()
                & (pl.col("paid_at") < pl.col("created_at"))
            )
            .then(None)
            .otherwise(pl.col("paid_at"))
            .alias("paid_at")
        )

        df_clean = df.filter(pl.col("id").is_not_null())
        n_out = df_clean.height

        aggs = df_clean.group_by(["name", "created_at"]).agg(
            pl.len().cast(pl.Int64).alias("tx_count_by_name_day"),
            pl.col("amount").sum().alias("amount_sum_by_name_day"),
            pl.col("amount").mean().alias("amount_avg_by_name_day"),
            (pl.col("status") == "paid").sum().cast(pl.Int64).alias("paid_count_by_name_day"),
        )
        df_master = df_clean.join(aggs, on = ["name", "created_at"], how = "left")

        buf = BytesIO()
        df_master.write_parquet(buf, compression = "snappy")
        buf.seek(0)
        s3.put_object(
            Bucket = BUCKET_BRONZE,
            Key = BRONZE_PARQUET_KEY,
            Body = buf.getvalue(),
            ContentType = "application/octet-stream",
        )

        print(f"Filas in: {n_in} | Filas out: {n_out} | Descartadas: {n_in - n_out}")
        print("Resumen agregaciones (top 10 días por monto):")
        print(
            aggs.sort("amount_sum_by_name_day", descending = True, nulls_last = True).head(10)
        )
        return f"s3://{BUCKET_BRONZE}/{BRONZE_PARQUET_KEY}"

    @task
    def expose_in_trino() -> str:
        from trino.dbapi import connect

        conn = connect(
            host = TRINO_HOST, port = TRINO_PORT, user = TRINO_USER, catalog = TRINO_CATALOG
        )
        cur = conn.cursor()

        cur.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}
            WITH (location = 's3a://{BUCKET_BRONZE}/')
            """
        )
        cur.fetchall()

        cur.execute(
            f"DROP TABLE IF EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}"
        )
        cur.fetchall()

        cur.execute(
            f"""
            CREATE TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} (
                id varchar,
                name varchar,
                company_id varchar,
                amount double,
                status varchar,
                created_at date,
                paid_at date,
                tx_count_by_name_day bigint,
                amount_sum_by_name_day double,
                amount_avg_by_name_day double,
                paid_count_by_name_day bigint
            )
            WITH (
                external_location = 's3a://{BUCKET_BRONZE}/master',
                format = 'PARQUET'
            )
            """
        )
        cur.fetchall()

        cur.execute(
            f"SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}"
        )
        (n,) = cur.fetchone()
        return f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} ({n} filas)"

    setup_task = setup_buckets()
    landing_task = ingest_to_landing()
    bronze_task = transform_to_bronze()
    trino_task = expose_in_trino()

    setup_task >> landing_task >> bronze_task >> trino_task

etl_engineer_challenge()