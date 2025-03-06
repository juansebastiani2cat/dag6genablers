from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time
import pandas as pd
import psycopg2
import pytz
from influxdb_client import InfluxDBClient
import re

#Test de actualizacion

# Configuración básica de DAG
config = {
    'org': "optare",
    'token': "KCbXUjsNG5ko_FVIa9vx2d9oS-WavbGP1I50UZ6xPN1Okzx6FIwZ3Az4GdTjq1B0v0dxbDR8lLS-u-Uq0Byj8A==",
    'influxdb_url': "http://influxdb.observability.svc.cluster.local:8086",
    
    'db_host': "postgres.observability.svc.cluster.local",
    'db_port': '5432',
    'db_database': "metrics",
    'db_user': "admin",
    'db_password': "admin1234",
    'db_table': "metrics_holo_app_1m",
    
    'bucket': "holo_metrics",
    'window_period': "1m",
    'holo_app_regex': "/^(HoloCapturerDepthRenderer|HoloCapturerEncoder|HoloCapturerPreparer)$/",
    'default_start_time': datetime(1970, 1, 1, tzinfo=pytz.UTC)
}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 22, tzinfo=pytz.UTC),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'end_date': datetime.combine(datetime.now().date(), time(18, 0)),  # Termina a las 18:00 del día actual
}

dag = DAG(
    'metrics_holo_app_1m',
    default_args=default_args,
    description='Metrics holo k8s 1m',
    schedule_interval=timedelta(minutes=1),
    tags=["i2cat_etl"],
    concurrency=5,  # Limita la concurrencia a 5 tareas
    max_active_runs=2  # Limita a 1 ejecución activa del DAG
)

# Tarea de ingesta
def ingest(ti):
    print("Ingesting data from InfluxDB")
    # Configuraciones y cliente de InfluxDB

    pg_conn = psycopg2.connect(host=config['db_host'], database=config['db_database'], user=config['db_user'], password=config['db_password'])
    pg_cursor = pg_conn.cursor()

# Asegúrate de que la tabla exista en PostgreSQL
# Asegúrate de que la tabla exista en PostgreSQL
    pg_cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {config['db_table']} (
            _time TIMESTAMP WITH TIME ZONE NOT NULL,
            value_mean_RGBD_latency_ms FLOAT,
            value_mean_fps FLOAT,
            value_mean_sessions_count FLOAT,
            PRIMARY KEY (_time)
        );
    ''')
    pg_conn.commit()
    # Obtener la última marca de tiempo de sincronización de PostgreSQL
    table = config['db_table']
    pg_cursor.execute(f'SELECT MAX(_time) FROM {table};')
    last_sync_time = pg_cursor.fetchone()[0]
    pg_cursor.close()
    pg_conn.close()

    # Si nunca se han sincronizado datos, establecer una fecha de inicio por defecto
    if last_sync_time is None:
        last_sync_time = datetime(1970, 1, 1, tzinfo=pytz.UTC)

    timeRangeStart = last_sync_time.isoformat()
    timeRangeStop = datetime.now(pytz.UTC).isoformat()


    query = f'''
    from(bucket: "{config['bucket']}")
    |> range(start: {timeRangeStart}, stop: {timeRangeStop})
    |> filter(fn: (r) =>
            r["_measurement"] == "RGBD_latency_ms" or
            r["_measurement"] == "fps")
    |> filter(fn: (r) => r["_field"] == "gauge")
    |> filter(fn: (r) => r["exported_job"] =~ {config['holo_app_regex']})
    |> aggregateWindow(every: {config['window_period']}, fn: mean, createEmpty: false)
    |> yield(name: "mean_rgbd_fps")

    from(bucket: "{config['bucket']}")
    |> range(start: {timeRangeStart}, stop: {timeRangeStop})
    |> filter(fn: (r) => r["_measurement"] == "sessions_count")
    |> filter(fn: (r) => r["_field"] == "gauge")
    |> aggregateWindow(every: {config['window_period']}, fn: mean, createEmpty: false)
    |> yield(name: "mean_sessions_count")
    '''

        
    client = InfluxDBClient(url=config['influxdb_url'],token=config['token'],org=config['org'])
    query_api = client.query_api()
    result = query_api.query_data_frame(query=query, org=config['org'])
    client.close()

    # Guardar el resultado en XComs
    ti.xcom_push(key='raw_data', value=result)

# Tarea de transformación
def transform(ti):
    print("Transforming data")

    # Recoger los datos del paso anterior
    raw_data = ti.xcom_pull(key='raw_data', task_ids='ingest_task')

    if isinstance(raw_data, list):  # Verifica si es una lista
        raw_data = pd.concat(raw_data, ignore_index=True)
    else:
        raw_data = raw_data  # Si no es lista, asume que ya es un DataFrame

    # Pivotar el DataFrame basado en el campo 'result'
    pivoted_result = raw_data.pivot_table(
        index=['_time', '_start', '_stop', 'service.name', '_measurement'],
        columns='result',
        values='_value'
    ).reset_index()

    # Renombrar las columnas para mayor claridad
    pivoted_result.rename(columns={
        'mean': 'value_mean'
    }, inplace=True)

    raw_data = pivoted_result

    
    # Seleccionar las columnas relevantes y crear una copia explícita
    columns = ['_time', '_measurement','service.name', 'mean_rgbd_fps','mean_sessions_count']
    holo_app = raw_data[columns].copy()  # Agrega .copy() para evitar SettingWithCopyWarning

    # Ordenar los valores y reiniciar el índice
    holo_app_sorted = holo_app.sort_values(by='_time', ascending=False).reset_index(drop=True)

    # Concatenar valores para modificar '_measurement'
    holo_app_sorted.rename(columns={'service.name': 'service_name'}, inplace=True)


    # Agrupar por _time, _measurement y service_name
    grouped_df = holo_app_sorted.groupby(
        ["_time", "_measurement", "service_name"]
    ).agg(
        # Calcular el promedio (mean) de las columnas relevantes
        mean_rgbd_fps=("mean_rgbd_fps", "mean"),
        mean_sessions_count=("mean_sessions_count", "mean")
    ).reset_index()

    # Ordenar los resultados por _time en orden descendente
    grouped_df_sorted = grouped_df.sort_values(by='_time', ascending=False).reset_index(drop=True)

    # Crear una nueva columna 'value_mean' combinando las columnas 'mean_rgbd_fps' y 'mean_sessions_count'
    grouped_df_sorted['value_mean'] = grouped_df_sorted['mean_rgbd_fps'].combine_first(grouped_df_sorted['mean_sessions_count'])

    # Eliminar las columnas originales
    grouped_df_sorted = grouped_df_sorted.drop(columns=['mean_rgbd_fps', 'mean_sessions_count'])

    # Pivotar los datos incluyendo `one_vm_name` y `one_vm_worker`
    pivot_df = grouped_df_sorted.pivot_table(
        index=['_time'], 
        columns='_measurement', 
        values=['value_mean'],  # Incluir las métricas
        dropna=False
    ).reset_index()

    # Opcional: Eliminar filas con valores NaN
    pivot_df = pivot_df.dropna()
    pivot_df.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in pivot_df.columns]

    # Convertir la columna _time a tipo datetime
    pivot_df['_time_'] = pd.to_datetime(pivot_df['_time_'])

    # Filtrar los resultados que tengan intervalos de 1 minuto exacto
    filtered_df = pivot_df[(pivot_df['_time_'].dt.second == 0)].copy()
    transformed_data = filtered_df
    
    transformed_data.columns = transformed_data.columns.str.replace(r'\.', '_', regex=True)

    # Guardar los datos transformados para la siguiente tarea
    ti.xcom_push(key='transformed_data', value=transformed_data)

# Tarea de carga
def load(ti):
    print("Loading data into PostgreSQL")
    # Recoger los datos transformados
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_task')
    # Conexión con PostgreSQL y carga de datos
    # Configuraciones de la base de datos
    
    pg_conn = psycopg2.connect(host=config['db_host'], database=config['db_database'], user=config['db_user'], password=config['db_password'])
    pg_cursor = pg_conn.cursor()

# Número de resultados a insertar
    inserted_records_count = 0  # Contador de registros insertados

    # Insertar datos transformados (simulando)
    for index, row in transformed_data.iterrows():
        
        # Extraer los valores de las columnas del DataFrame
        _time = row['_time_']
        value_mean_RGBD_latency_ms = row['value_mean_RGBD_latency_ms']
        value_mean_fps = row['value_mean_fps']
        value_mean_sessions_count = row['value_mean_sessions_count']

        # Insertar datos en PostgreSQL
        insert_query = f'''
            INSERT INTO {config['db_table']} (
                _time,
                value_mean_RGBD_latency_ms,
                value_mean_fps,
                value_mean_sessions_count
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (_time) DO NOTHING;
        '''
        
        pg_cursor.execute(insert_query, (
            _time,
            value_mean_RGBD_latency_ms,
            value_mean_fps,
            value_mean_sessions_count
        ))

        inserted_records_count += 1

    print(f'Número de registros insertados: {inserted_records_count}')
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()



# Definición de tareas en Airflow
ingest_task = PythonOperator(
    task_id='ingest_task',
    python_callable=ingest,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

# Establecer dependencias de las tareas
ingest_task >> transform_task >> load_task
