from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time
import warnings
from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction
warnings.simplefilter("ignore", MissingPivotFunction)


import pandas as pd
import re
from datetime import datetime, timedelta
import os
import psycopg2
import pytz

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 6, tzinfo=pytz.UTC), #datetime.combine(datetime.now().date(), time(8, 0)),  # Empieza a las 8:00 del día actual
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'end_date': datetime.combine(datetime.now().date(), time(18, 0)),  # Termina a las 18:00 del día actual
}

dag = DAG(
    'sync_influxdb_to_postgres',
    default_args=default_args,
    description='Sync InfluxDB to PostgreSQL',
    schedule_interval=timedelta(minutes=10),
    tags=["i2cat_etl"],
    concurrency=5,  # Limita la concurrencia a 5 tareas
    max_active_runs=2  # Limita a 1 ejecución activa del DAG
)

def ingest(*args, **kwargs):
    '''Ingest data from InfluxDB2'''
    print("task_ingest")

    org = "optare"
    token = "KCbXUjsNG5ko_FVIa9vx2d9oS-WavbGP1I50UZ6xPN1Okzx6FIwZ3Az4GdTjq1B0v0dxbDR8lLS-u-Uq0Byj8A=="
    url="http://influxdb.observability-backend.svc.cluster.local:8086"

    db_host = "postgres-feast.observability-backend.svc.cluster.local"
    db_port = '5432'
    db_database = "postgres"
    db_user = "feast"
    db_password = "feast"
    db_table = 'metrics_service'

    client = InfluxDBClient(url=url,token=token,org=org)

    pg_conn = psycopg2.connect(host=db_host, database=db_database, user=db_user, password=db_password)
    pg_cursor = pg_conn.cursor()

    # Asegúrate de que la tabla exista en PostgreSQL
    pg_cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {db_table} (
        _measurement VARCHAR NOT NULL,
        _time TIMESTAMP WITH TIME ZONE NOT NULL,
        deployment VARCHAR NOT NULL,
        container_cpu_time FLOAT,
        container_cpu_utilization FLOAT,
        container_memory_usage FLOAT,
        k8s_pod_cpu_time FLOAT,
        k8s_pod_cpu_utilization FLOAT,
        k8s_pod_network_io FLOAT,
        PRIMARY KEY (_time, deployment, _measurement)
        );
    ''')
    pg_conn.commit()

    # Obtener la última marca de tiempo de sincronización de PostgreSQL
    pg_cursor.execute(f'SELECT MAX(_time) FROM {db_table};')
    last_sync_time = pg_cursor.fetchone()[0]

    # Si nunca se han sincronizado datos, establecer una fecha de inicio por defecto
    if last_sync_time is None:
        last_sync_time = datetime(1970, 1, 1, tzinfo=pytz.UTC)

    bucket = "metrics"
    timeRangeStart = last_sync_time.isoformat()
    timeRangeStop = datetime.now(pytz.UTC).isoformat()
    windowPeriod = "10m"
    services = "/^(nginx-deployment.*|java-.*)$/"
    #services = "/^(java-.*)$/"

    query = f'''
                from(bucket: "{bucket}")
                |> range(start: {timeRangeStart}, stop: {timeRangeStop})
                |> filter(fn: (r) => 
                    r["_measurement"] == "container.cpu.utilization" or
                    r["_measurement"] == "k8s.pod.cpu.utilization" or
                    r["_measurement"] == "container.cpu.time" or
                    r["_measurement"] == "k8s.pod.cpu.time" or
                    r["_measurement"] == "k8s.pod.network.io" or
                    r["_measurement"] == "container.memory.usage" or
                    r["_measurement"] == "k8.pod.memory.usage" or
                    r["_measurement"] == "k8.pod.network.io"
                    ) 
                |> filter(fn: (r) => r["_field"] == "gauge" or r["_field"] == "counter")
                |> filter(fn: (r) => r["k8s.pod.name"] =~ {services})
                |> aggregateWindow(every: {windowPeriod}, fn: mean, createEmpty: false)
                '''
    
    query_api = client.query_api()
    result = query_api.query_data_frame(query=query, org=org)
    
    # Concatenar los DataFrames
    concatenated_df = pd.concat(result, ignore_index=True)

    # Eliminar la columna 'table'
    concatenated_df = concatenated_df.drop(columns=['table'])
    df = concatenated_df

    # Pivotar el DataFrame
    pivot_df = df.pivot_table(index=['_start', '_stop', '_time', 'k8s.namespace.name', 'k8s.node.name', 'k8s.pod.name'], 
                          columns='_measurement', 
                          values='_value').reset_index()
    
    # Convertir la columna _time a tipo datetime
    pivot_df['_time'] = pd.to_datetime(pivot_df['_time'])

    # Filtrar los resultados que no tengan intervalos de 10 minutos
    filtered_df = pivot_df[(pivot_df['_time'].dt.minute % 10 == 0) & (pivot_df['_time'].dt.second == 0)].copy()

    # Usar una expresión regular para separar en dos columnas distintas
    filtered_df[['deployment', 'pod_id']] = filtered_df['k8s.pod.name'].str.extract(r'([a-zA-Z0-9\-]+)-([a-zA-Z0-9]+-[a-zA-Z0-9]+)')

    # Eliminar la columna original 'k8s.pod.name'
    filtered_df.drop(columns=['k8s.pod.name'], inplace=True)

    # Eliminar la columna original 'k8s.pod.name'
    filtered_df.drop(columns=['pod_id'], inplace=True)

    # Agrupar por '_time' y 'deployment' y calcular la media
    numeric_cols = ['container.cpu.time','container.cpu.utilization','container.memory.usage','k8s.pod.cpu.time','k8s.pod.cpu.utilization','k8s.pod.network.io']
    df_grouped = filtered_df.groupby(['_time', 'deployment'])[numeric_cols].mean().reset_index()

    # Procesar los resultados obtenidos
    inserted_records_count = 0  # Contador de registros insertados

    # Insertar los datos filtrados en PostgreSQL
    for index, row in df_grouped.iterrows():
        # Extraer los valores de las columnas del DataFrame
        _time = row['_time']
        deployment = row['deployment']
        container_cpu_time = row.get('container.cpu.time', None)
        container_cpu_utilization = row.get('container.cpu.utilization', None)
        container_memory_usage = row.get('container.memory.usage', None)
        k8s_pod_cpu_time = row.get('k8s.pod.cpu.time', None)
        k8s_pod_cpu_utilization = row.get('k8s.pod.cpu.utilization', None)
        k8s_pod_network_io = row.get('k8s.pod.network.io', None)

        # Insertar datos en PostgreSQL
        insert_query = f'''
            INSERT INTO {db_table} (
                _measurement, _time, deployment, 
                container_cpu_time, container_cpu_utilization, container_memory_usage, 
                k8s_pod_cpu_time, k8s_pod_cpu_utilization, k8s_pod_network_io
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (_time, deployment, _measurement) DO NOTHING;
        '''
        pg_cursor.execute(insert_query, (
            'combined_measurement', _time, deployment, 
            container_cpu_time, container_cpu_utilization, container_memory_usage, 
            k8s_pod_cpu_time, k8s_pod_cpu_utilization, k8s_pod_network_io
        ))
        inserted_records_count += 1

    pg_conn.commit()

    pg_cursor.close()
    pg_conn.close()
    client.close()
    

ingest_task = PythonOperator(
 task_id='ingest_task',
 python_callable=ingest,
 dag=dag,
)






