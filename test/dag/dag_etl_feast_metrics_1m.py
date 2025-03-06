from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time
import pandas as pd
import psycopg2
import pytz
from influxdb_client import InfluxDBClient

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
    'db_table': "metrics_feast_1m",
    
    'bucket': "metrics",
    'window_period': "10m",
     'services_regex': "/^(feature-store-deployment-.*)$/",
    'default_start_time': datetime(1970, 1, 1, tzinfo=pytz.UTC)
}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.combine(datetime.now().date(), time(10, 0)),  # Empieza a las 8:00 del día actual
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'end_date': datetime.combine(datetime.now().date(), time(18, 0)),  # Termina a las 18:00 del día actual
}

dag = DAG(
    'sync_etl_feast_telemetry_1m',
    default_args=default_args,
    description='Sync Feast Telemetry 1m',
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
    pg_cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {config['db_table']} (
            _time TIMESTAMP WITH TIME ZONE NOT NULL,
            offline_http_server_duration FLOAT,
            online_http_server_duration FLOAT,
            offline_work_counter FLOAT,
            online_work_counter FLOAT,
            k8s_pod_cpu_time FLOAT,
            k8s_pod_cpu_utilization FLOAT,
            k8s_pod_memory_available FLOAT,
            k8s_pod_memory_major_page_faults FLOAT,
            k8s_pod_memory_page_faults FLOAT,
            k8s_pod_memory_rss FLOAT,
            k8s_pod_memory_usage FLOAT,
            k8s_pod_memory_working_set FLOAT,
            k8s_pod_network_errors FLOAT,
            k8s_pod_network_io FLOAT,
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
            r["_measurement"] == "k8s.pod.cpu.utilization" or
            r["_measurement"] == "k8s.pod.memory.usage" or
            r["_measurement"] == "k8s.pod.memory.available" or
            r["_measurement"] == "k8s.pod.memory.major_page_faults" or
            r["_measurement"] == "k8s.pod.memory.page_faults" or
            r["_measurement"] == "k8s.pod.memory.rss" or
            r["_measurement"] == "k8s.pod.memory.working_set"
                )
        |> filter(fn: (r) => r["_field"] == "gauge")
        |> filter(fn: (r) => r["k8s.pod.name"] =~ {config['services_regex']})
        |> aggregateWindow(every: {config['window_period']}, fn: mean, createEmpty: false)
        |> yield(name: "mean")
        '''

    query2 = f'''
        from(bucket: "{config['bucket']}")
        |> range(start: {timeRangeStart}, stop: {timeRangeStop})
        |> filter(fn: (r) =>
            r["_measurement"] == "k8s.pod.cpu.time" or
            r["_measurement"] == "offline.work.counter" or
            r["_measurement"] == "online.work.counter" or
            r["_measurement"] == "k8s.pod.network.io" or
            r["_measurement"] == "k8s.pod.network.errors" 
                )
        |> filter(fn: (r) => r["_field"] == "counter")
        |> filter(fn: (r) => r["k8s.pod.name"] =~ {config['services_regex']})
        |> aggregateWindow(every: {config['window_period']}, fn: mean, createEmpty: false)
        |> yield(name: "mean")
        '''

    query3 = f'''
        from(bucket: "{config['bucket']}")
        |> range(start: {timeRangeStart}, stop: {timeRangeStop})
        |> filter(fn: (r) =>
            r["_measurement"] == "offline.http.server.duration" or
            r["_measurement"] == "online.http.server.duration" 
                )
        |> filter(fn: (r) => r["_field"] == "sum")
        |> filter(fn: (r) => r["k8s.pod.name"] =~ {config['services_regex']})
        |> aggregateWindow(every: {config['window_period']}, fn: mean, createEmpty: false)
        |> yield(name: "mean")
        '''
        
    client = InfluxDBClient(url=config['influxdb_url'],token=config['token'],org=config['org'])
    query_api = client.query_api()
    result = query_api.query_data_frame(query=query, org=config['org'])
    result2 = query_api.query_data_frame(query=query2, org=config['org'])
    result3 = query_api.query_data_frame(query=query3, org=config['org'])


    client.close()

    if isinstance(result, list):  # Verifica si es una lista
        result = pd.concat(result, ignore_index=True)
    else:
        result = result  # Si no es lista, asume que ya es un DataFrame
        result.head(2)

    # Combina todos los DataFrames en uno solo
    if isinstance(result2, list):  # Verifica si es una lista
        result2 = pd.concat(result2, ignore_index=True)
    else:
        result2 = result2  # Si no es lista, asume que ya es un DataFrame
        result2.head(2)

    # Combina todos los DataFrames en uno solo
    if isinstance(result3, list):  # Verifica si es una lista
        result3 = pd.concat(result3, ignore_index=True)
    else:
        result3 = result3  # Si no es lista, asume que ya es un DataFrame
        result3.head(2)


    columns = ['_time','_measurement','_value']
    result = result[columns]
    result2 = result2[columns]
    result3 = result3[columns]

    
    # Pivotar el DataFrame
    pivot_df = result.pivot_table(index=['_time'], columns='_measurement', values='_value', dropna=False).reset_index()
    pivot_df2 = result2.pivot_table(index=['_time'], columns='_measurement', values='_value', dropna=False).reset_index()
    pivot_df3 = result3.pivot_table(index=['_time'], columns='_measurement', values='_value', dropna=False).reset_index()

    merged_result = pd.merge(pivot_df, pivot_df2, on='_time', how='inner')  # You can change 'outer' to 'inner', 'left', or 'right' as needed
    merged_result = pd.merge(merged_result, pivot_df3, on='_time', how='inner')  # You can change 'outer' to 'inner', 'left', or 'right' as needed
    # Guardar el resultado en XComs
    ti.xcom_push(key='raw_data', value=merged_result)

# Tarea de transformación
def transform(ti):
    print("Transforming data")

    # Recoger los datos del paso anterior
    raw_data = ti.xcom_pull(key='raw_data', task_ids='ingest_task')
    
    
    # Convertir la columna _time a tipo datetime
    raw_data['_time'] = pd.to_datetime(raw_data['_time'])

    # Filtrar los resultados que tengan intervalos de 10 minuto exactos
    filtered_df = raw_data[(raw_data['_time'].dt.minute % 10 == 0) & (raw_data['_time'].dt.second == 0)].copy()
    transformed_data = filtered_df

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
        _time = row['_time']
        k8s_pod_cpu_utilization = row['k8s.pod.cpu.utilization'] 
        k8s_pod_cpu_time = row['k8s.pod.cpu.time'] 
        k8s_pod_memory_major_page_faults = row['k8s.pod.memory.major_page_faults'] 
        k8s_pod_memory_page_faults = row['k8s.pod.memory.page_faults'] 
        k8s_pod_memory_rss	 = row['k8s.pod.memory.rss'] 
        k8s_pod_memory_usage	 = row['k8s.pod.memory.usage'] 
        k8s_pod_memory_available	 = row['k8s.pod.memory.available'] 
        k8s_pod_memory_working_set	 = row['k8s.pod.memory.working_set'] 
        offline_work_counter	= row['offline.work.counter'] 
        online_work_counter = row['online.work.counter'] 
        k8s_pod_network_io = row['k8s.pod.network.io'] 
        k8s_pod_network_errors = row['k8s.pod.network.errors'] 
        offline_http_server_duration = row['offline.http.server.duration']
        online_http_server_duration = row['online.http.server.duration']

        # Insertar datos en PostgreSQL
        insert_query = f'''
            INSERT INTO {config['db_table']} (
                _time,
                k8s_pod_cpu_utilization,
                k8s_pod_cpu_time,
                k8s_pod_memory_major_page_faults,
                k8s_pod_memory_usage,
                k8s_pod_memory_available,
                k8s_pod_memory_working_set,
                offline_work_counter,
                online_work_counter,
                k8s_pod_network_io,
                k8s_pod_memory_rss,
                k8s_pod_network_errors,
                offline_http_server_duration,
                online_http_server_duration,
                k8s_pod_memory_page_faults
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (_time) DO NOTHING;
            '''
        pg_cursor.execute(insert_query, (
            _time,
            k8s_pod_cpu_utilization,
            k8s_pod_cpu_time,
            k8s_pod_memory_major_page_faults,
            k8s_pod_memory_usage,
            k8s_pod_memory_available,
            k8s_pod_memory_working_set,
            offline_work_counter,
            online_work_counter,
            k8s_pod_network_io,
            k8s_pod_memory_rss,
            k8s_pod_network_errors,
            offline_http_server_duration,
            online_http_server_duration,
            k8s_pod_memory_page_faults
            )
            )
        
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
