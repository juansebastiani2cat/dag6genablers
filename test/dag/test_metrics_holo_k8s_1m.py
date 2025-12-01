from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time
import pandas as pd
import psycopg2
import pytz
from influxdb_client import InfluxDBClient

# =========================
# Configuración básica
# =========================

config = {
    "org": "optare",
    "token": "KCbXUjsNG5ko_FVIa9vx2d9oS-WavbGP1I50UZ6xPN1Okzx6FIwZ3Az4GdTjq1B0v0dxbDR8lLS-u-Uq0Byj8A==",
    "influxdb_url": "http://influxdb.observability.svc.cluster.local:8086",

    "db_host": "postgres.observability.svc.cluster.local",
    "db_port": "5432",
    "db_database": "metrics",
    "db_user": "admin",
    "db_password": "admin1234",
    "db_table": "metrics_holo_k8s_1m",

    "bucket": "metrics",
    "window_period": "1m",

    # Nodo observado (ajusta este valor si cambias de nodo)
    "node_name": "oneke-ip-10-10-8-107",

    # Valor por defecto para primera sincronización
    "default_start_time": datetime(1970, 1, 1, tzinfo=pytz.UTC),
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 01, tzinfo=pytz.UTC),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "test_metrics_holo_k8s_1m",
    default_args=default_args,
    description="Metrics holo k8s 1m (node CPU → Postgres)",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    tags=["i2cat_etl"],
    concurrency=5,
    max_active_runs=2,
)


# =========================
# Tarea de INGESTA (Influx → XCom)
# =========================

def ingest(ti, **kwargs):
    print("Ingesting data from InfluxDB")

    # Conexión inicial a PostgreSQL para asegurar tabla y obtener último _time
    pg_conn = psycopg2.connect(
        host=config["db_host"],
        database=config["db_database"],
        user=config["db_user"],
        password=config["db_password"],
    )
    pg_cursor = pg_conn.cursor()

    # Crear tabla si no existe
    pg_cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {config['db_table']} (
            _time TIMESTAMP WITH TIME ZONE NOT NULL,
            one_vm_name VARCHAR NOT NULL,
            one_vm_worker VARCHAR NOT NULL,
            value_max_orchestrator_k8s_pod_cpu_utilization FLOAT,
            value_mean_orchestrator_k8s_pod_cpu_utilization FLOAT,
            value_min_orchestrator_k8s_pod_cpu_utilization FLOAT,
            PRIMARY KEY (_time, one_vm_name)
        );
    """)
    pg_conn.commit()

    # Obtener la última marca de tiempo de sincronización
    table = config["db_table"]
    pg_cursor.execute(f"SELECT MAX(_time) FROM {table};")
    last_sync_time = pg_cursor.fetchone()[0]

    pg_cursor.close()
    pg_conn.close()

    # Si nunca se han sincronizado datos
    if last_sync_time is None:
        last_sync_time = config["default_start_time"]

    # Formatear tiempos en RFC3339 para Flux
    last_sync_time = last_sync_time.astimezone(pytz.UTC)
    start_str = last_sync_time.isoformat().replace("+00:00", "Z")

    now_utc = datetime.now(pytz.UTC)
    stop_str = now_utc.isoformat().replace("+00:00", "Z")

    # Query Flux para métricas de nodo
    query = f'''
from(bucket: "{config['bucket']}")
  |> range(start: time(v: "{start_str}"), stop: time(v: "{stop_str}"))
  |> filter(fn: (r) => r._measurement == "k8s.node.cpu.utilization")
  |> filter(fn: (r) => r._field == "gauge")
  |> filter(fn: (r) => r["k8s.node.name"] == "{config['node_name']}")
  |> keep(columns: ["_time", "_value", "k8s.node.name", "one_vm_worker", "one_vm_name"])
  |> sort(columns: ["_time"])
'''

    client = InfluxDBClient(
        url=config["influxdb_url"],
        token=config["token"],
        org=config["org"],
    )
    query_api = client.query_api()

    # Puede devolver un DataFrame o lista de DataFrames
    result = query_api.query_data_frame(query=query, org=config["org"])
    client.close()

    ti.xcom_push(key="raw_data", value=result)


# =========================
# Tarea de TRANSFORMACIÓN (XCom → DataFrame agregada)
# =========================

def transform(ti, **kwargs):
    print("Transforming data")

    raw_data = ti.xcom_pull(key="raw_data", task_ids="ingest_task")

    # Unificar a un único DataFrame
    if isinstance(raw_data, list):
        raw_data = pd.concat(raw_data, ignore_index=True)
    else:
        raw_data = raw_data  # ya es DataFrame

    if raw_data is None or raw_data.empty:
        print("No data from InfluxDB in this interval.")
        ti.xcom_push(key="transformed_data", value=pd.DataFrame())
        return

    # Asegurar columnas necesarias
    if "one_vm_name" not in raw_data.columns:
        # Fallback: usar el nombre del nodo como VM lógica
        raw_data["one_vm_name"] = raw_data.get("k8s.node.name", "unknown_vm")

    if "one_vm_worker" not in raw_data.columns:
        raw_data["one_vm_worker"] = "unknown_worker"

    # Mantener solo columnas relevantes
    cols_to_keep = ["_time", "_value", "k8s.node.name", "one_vm_name", "one_vm_worker"]
    cols_to_keep = [c for c in cols_to_keep if c in raw_data.columns]
    df = raw_data[cols_to_keep].copy()

    # Convertir tiempo a datetime y redondear a buckets de 1 minuto
    df["_time"] = pd.to_datetime(df["_time"])
    df["time_1m"] = df["_time"].dt.floor("1min")

    # Agrupar por minuto + VM + worker y calcular mean/min/max
    grouped = (
        df.groupby(["time_1m", "one_vm_name", "one_vm_worker"])
        .agg(
            value_mean=("_value", "mean"),
            value_min=("_value", "min"),
            value_max=("_value", "max"),
        )
        .reset_index()
    )

    # Renombrar columnas para que coincidan con el esquema de PostgreSQL
    grouped = grouped.rename(
        columns={
            "time_1m": "_time",
            "value_mean": "value_mean_orchestrator_k8s_pod_cpu_utilization",
            "value_min": "value_min_orchestrator_k8s_pod_cpu_utilization",
            "value_max": "value_max_orchestrator_k8s_pod_cpu_utilization",
        }
    )

    transformed_data = grouped
    ti.xcom_push(key="transformed_data", value=transformed_data)


# =========================
# Tarea de CARGA (DataFrame → PostgreSQL)
# =========================

def load(ti, **kwargs):
    print("Loading data into PostgreSQL")

    transformed_data = ti.xcom_pull(key="transformed_data", task_ids="transform_task")

    if transformed_data is None or len(transformed_data) == 0:
        print("No transformed data to load.")
        return

    # Conexión a PostgreSQL
    pg_conn = psycopg2.connect(
        host=config["db_host"],
        database=config["db_database"],
        user=config["db_user"],
        password=config["db_password"],
    )
    pg_cursor = pg_conn.cursor()

    inserted_records_count = 0

    for _, row in transformed_data.iterrows():
        _time = row["_time"]
        one_vm_name = row["one_vm_name"]
        one_vm_worker = row["one_vm_worker"]

        value_max = row["value_max_orchestrator_k8s_pod_cpu_utilization"]
        value_mean = row["value_mean_orchestrator_k8s_pod_cpu_utilization"]
        value_min = row["value_min_orchestrator_k8s_pod_cpu_utilization"]

        insert_query = f"""
            INSERT INTO {config['db_table']}
            (
                _time,
                one_vm_name,
                one_vm_worker,
                value_max_orchestrator_k8s_pod_cpu_utilization,
                value_mean_orchestrator_k8s_pod_cpu_utilization,
                value_min_orchestrator_k8s_pod_cpu_utilization
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (_time, one_vm_name) DO NOTHING;
        """

        pg_cursor.execute(
            insert_query,
            (_time, one_vm_name, one_vm_worker, value_max, value_mean, value_min),
        )
        inserted_records_count += 1

    print(f"Número de registros insertados: {inserted_records_count}")

    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()


# =========================
# Definición de tareas
# =========================

ingest_task = PythonOperator(
    task_id="ingest_task",
    python_callable=ingest,
    provide_context=True,  # necesario para recibir 'ti' en la función
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_task",
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_task",
    python_callable=load,
    provide_context=True,
    dag=dag,
)

# Dependencias
ingest_task >> transform_task >> load_task
