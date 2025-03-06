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
    'influxdb_url': "http://influxdb.observability-backend.svc.cluster.local:8086",
    
    'db_host': "postgres-feast.observability-backend.svc.cluster.local",
    'db_port': '5432',
    'db_database': "postgres",
    'db_user': "feast",
    'db_password': "feast",
    'db_table': "metrics_service_extend",
    
    'bucket': "metrics",
    'window_period': "10m",
    'services_regex': "/^(nginx-deployment-.*|java-.*|producer-node-.*)$/",
    'default_start_time': datetime(1970, 1, 1, tzinfo=pytz.UTC)
}


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
    'sync_etl_influxdb_to_postgres_extend',
    default_args=default_args,
    description='Sync InfluxDB to PostgreSQL infra extend',
    schedule_interval=timedelta(minutes=10),
    tags=["i2cat_etl"],
    concurrency=5,  # Limita la concurrencia a 5 tareas
    max_active_runs=2  # Limita a 1 ejecución activa del DAG
)

# Tarea de ingesta
def ingest(ti):
    print("Ingesting data from InfluxDB")
    # Configuraciones y cliente de InfluxDB

    last_sync_time = None
    pg_conn = psycopg2.connect(host=config['db_host'], database=config['db_database'], user=config['db_user'], password=config['db_password'])
    pg_cursor = pg_conn.cursor()
    # Obtener la última marca de tiempo de sincronización de PostgreSQL
    try:
        table = config['db_table']
        pg_cursor.execute(f'SELECT MAX(_time) FROM {table};')
        last_sync_time = pg_cursor.fetchone()[0]
        pg_cursor.close()
        pg_conn.close()
    except:
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
                    r._measurement == "container.cpu.time" or
                    r._measurement == "container.cpu.utilization" or
                    r._measurement == "container.filesystem.available" or
                    r._measurement == "container.filesystem.capacity" or
                    r._measurement == "container.filesystem.usage" or
                    r._measurement == "container.memory.available" or
                    r._measurement == "container.memory.major_page_faults" or
                    r._measurement == "container.memory.page_faults" or
                    r._measurement == "container.memory.rss" or
                    r._measurement == "container.memory.usage" or
                    r._measurement == "container.memory.working_set" or
                    r._measurement == "k8s.pod.cpu.time" or
                    r._measurement == "k8s.pod.cpu.utilization" or
                    r._measurement == "k8s.pod.filesystem.available" or
                    r._measurement == "k8s.pod.filesystem.capacity" or
                    r._measurement == "k8s.pod.filesystem.usage" or
                    r._measurement == "k8s.pod.memory.available" or
                    r._measurement == "k8s.pod.memory.major_page_faults" or
                    r._measurement == "k8s.pod.memory.page_faults" or
                    r._measurement == "k8s.pod.memory.rss" or
                    r._measurement == "k8s.pod.memory.usage" or
                    r._measurement == "k8s.pod.memory.working_set" or
                    r._measurement == "k8s.pod.network.errors" or
                    r._measurement == "k8s.pod.network.io" or
                    r._measurement == "k8s.volume.available" or
                    r._measurement == "k8s.volume.capacity" or
                    r._measurement == "k8s.volume.inodes" or
                    r._measurement == "k8s.volume.inodes.free" or
                    r._measurement == "k8s.volume.inodes.used" or
                    r._measurement == "k8s.container.cpu_limit" or
                    r._measurement == "k8s.container.cpu_request" or
                    r._measurement == "k8s.container.memory_limit" or
                    r._measurement == "k8s.container.memory_request" or 
                    r._measurement == "k8s.container.ready" or
                    r._measurement == "k8s.container.restarts" or
                    r._measurement == "k8s.pod.phase"
    )
                |> filter(fn: (r) => r["_field"] == "gauge" or r["_field"] == "counter" or r["_field"] == "flags")
                |> filter(fn: (r) => r["k8s.pod.name"] =~ {config['services_regex']})
                |> aggregateWindow(every: {config['window_period']}, fn: mean, createEmpty: false)
    
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
    
    # Concatenar los DataFrames
    concatenated_df = pd.concat(raw_data, ignore_index=True)

    # Eliminar la columna 'table'
    df = concatenated_df.drop(columns=['table'])
    
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
    numeric_cols = ['container.cpu.time',
                'container.cpu.utilization',
                'container.filesystem.available',
                'container.filesystem.capacity',
                'container.filesystem.usage',
                'container.memory.available',
                'container.memory.major_page_faults',
                'container.memory.page_faults',
                'container.memory.rss',
                'container.memory.usage',
                'container.memory.working_set',
                'k8s.container.cpu_limit',
                'k8s.container.cpu_request',
                'k8s.container.memory_limit',
                'k8s.container.memory_request',
                'k8s.container.ready',
                'k8s.container.restarts',
                'k8s.pod.cpu.time',
                'k8s.pod.cpu.utilization',
                'k8s.pod.filesystem.available',
                'k8s.pod.filesystem.capacity',
                'k8s.pod.filesystem.usage',
                'k8s.pod.memory.available',
                'k8s.pod.memory.major_page_faults',
                'k8s.pod.memory.page_faults',
                'k8s.pod.memory.rss',
                'k8s.pod.memory.usage',
                'k8s.pod.memory.working_set',
                'k8s.pod.network.errors',
                'k8s.pod.network.io',
                'k8s.pod.phase',
                'k8s.volume.available',
                'k8s.volume.capacity',
                'k8s.volume.inodes',
                'k8s.volume.inodes.free',
                'k8s.volume.inodes.used']
    
    df_grouped = filtered_df.groupby(['_time', 'deployment'])[numeric_cols].mean().reset_index()

    transformed_data = df_grouped

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

    # Asegúrate de que la tabla exista en PostgreSQL
    pg_cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {config['db_table']} (
            _measurement VARCHAR NOT NULL,
            _time TIMESTAMP WITH TIME ZONE NOT NULL,
            deployment VARCHAR NOT NULL,
            container_cpu_time FLOAT,
            container_cpu_utilization FLOAT,
            container_filesystem_available FLOAT,
            container_filesystem_capacity FLOAT,
            container_filesystem_usage FLOAT,
            container_memory_available FLOAT,
            container_memory_major_page_faults FLOAT,
            container_memory_page_faults FLOAT,
            container_memory_rss FLOAT,
            container_memory_usage FLOAT,
            container_memory_working_set FLOAT,
            k8s_pod_cpu_time FLOAT,
            k8s_pod_cpu_utilization FLOAT,
            k8s_pod_filesystem_available FLOAT,
            k8s_pod_filesystem_capacity FLOAT,
            k8s_pod_filesystem_usage FLOAT,
            k8s_pod_memory_available FLOAT,
            k8s_pod_memory_major_page_faults FLOAT,
            k8s_pod_memory_page_faults FLOAT,
            k8s_pod_memory_rss FLOAT,
            k8s_pod_memory_usage FLOAT,
            k8s_pod_memory_working_set FLOAT,
            k8s_pod_network_errors FLOAT,
            k8s_pod_network_io FLOAT,
            k8s_volume_available FLOAT,
            k8s_volume_capacity FLOAT,
            k8s_volume_inodes FLOAT,
            k8s_volume_inodes_free FLOAT,
            k8s_volume_inodes_used FLOAT,
            k8s_container_cpu_limit FLOAT,
            k8s_container_cpu_request FLOAT,
            k8s_container_memory_limit FLOAT,
            k8s_container_memory_request FLOAT,
            k8s_container_ready FLOAT,
            k8s_container_restarts INT,
            k8s_pod_phase VARCHAR,
            PRIMARY KEY (_time, deployment, _measurement)
            );
    ''')
    pg_conn.commit()

    # Número de resultados a insertar
    inserted_records_count = 0  # Contador de registros insertados
    # Insertar los datos filtrados en PostgreSQL
    for index, row in transformed_data.iterrows():
        # Extraer los valores de las columnas del DataFrame
        _time = row['_time']
        deployment = row['deployment']
        container_cpu_time = row.get('container.cpu.time', None)
        container_cpu_utilization = row.get('container.cpu.utilization', None)
        container_filesystem_available = row.get('container.filesystem.available', None)
        container_filesystem_capacity = row.get('container.filesystem.capacity', None)
        container_filesystem_usage = row.get('container.filesystem.usage', None)
        container_memory_available = row.get('container.memory.available', None)
        container_memory_major_page_faults = row.get('container.memory.major_page_faults', None)
        container_memory_page_faults = row.get('container.memory.page_faults', None)
        container_memory_rss = row.get('container.memory.rss', None)
        container_memory_usage = row.get('container.memory.usage', None)
        container_memory_working_set = row.get('container.memory.working_set', None)
        k8s_pod_cpu_time = row.get('k8s.pod.cpu.time', None)
        k8s_pod_cpu_utilization = row.get('k8s.pod.cpu.utilization', None)
        k8s_pod_filesystem_available = row.get('k8s.pod.filesystem.available', None)
        k8s_pod_filesystem_capacity = row.get('k8s.pod.filesystem.capacity', None)
        k8s_pod_filesystem_usage = row.get('k8s.pod.filesystem.usage', None)
        k8s_pod_memory_available = row.get('k8s.pod.memory.available', None)
        k8s_pod_memory_major_page_faults = row.get('k8s.pod.memory.major_page_faults', None)
        k8s_pod_memory_page_faults = row.get('k8s.pod.memory.page_faults', None)
        k8s_pod_memory_rss = row.get('k8s.pod.memory.rss', None)
        k8s_pod_memory_usage = row.get('k8s.pod.memory.usage', None)
        k8s_pod_memory_working_set = row.get('k8s.pod.memory.working_set', None)
        k8s_pod_network_errors = row.get('k8s.pod.network.errors', None)
        k8s_pod_network_io = row.get('k8s.pod.network.io', None)
        k8s_volume_available = row.get('k8s.volume.available', None)
        k8s_volume_capacity = row.get('k8s.volume.capacity', None)
        k8s_volume_inodes = row.get('k8s.volume.inodes', None)
        k8s_volume_inodes_free = row.get('k8s.volume.inodes.free', None)
        k8s_volume_inodes_used = row.get('k8s.volume.inodes.used', None)
        k8s_container_cpu_limit = row.get('k8s.container.cpu_limit', None)
        k8s_container_cpu_request = row.get('k8s.container.cpu_request', None)
        k8s_container_memory_limit = row.get('k8s.container.memory_limit', None)
        k8s_container_memory_request = row.get('k8s.container.memory_request', None)
        k8s_container_ready = row.get('k8s.container.ready', None)
        k8s_container_restarts = row.get('k8s.container.restarts', None)
        k8s_pod_phase = row.get('k8s.pod.phase', None)

        # Insertar datos en PostgreSQL
        insert_query = f'''
            INSERT INTO {config['db_table']} (
                _measurement, _time, deployment, 
                container_cpu_time, container_cpu_utilization, container_filesystem_available, 
                container_filesystem_capacity, container_filesystem_usage, container_memory_available, 
                container_memory_major_page_faults, container_memory_page_faults, container_memory_rss, 
                container_memory_usage, container_memory_working_set, k8s_pod_cpu_time, 
                k8s_pod_cpu_utilization, k8s_pod_filesystem_available, k8s_pod_filesystem_capacity, 
                k8s_pod_filesystem_usage, k8s_pod_memory_available, k8s_pod_memory_major_page_faults, 
                k8s_pod_memory_page_faults, k8s_pod_memory_rss, k8s_pod_memory_usage, 
                k8s_pod_memory_working_set, k8s_pod_network_errors, k8s_pod_network_io, 
                k8s_volume_available, k8s_volume_capacity, k8s_volume_inodes, 
                k8s_volume_inodes_free, k8s_volume_inodes_used, k8s_container_cpu_limit, 
                k8s_container_cpu_request, k8s_container_memory_limit, 
                k8s_container_memory_request, k8s_container_ready, k8s_container_restarts, 
                k8s_pod_phase
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (_time, deployment, _measurement) DO NOTHING;
        '''
        pg_cursor.execute(insert_query, (
            'combined_measurement', _time, deployment, 
            container_cpu_time, container_cpu_utilization, container_filesystem_available, 
            container_filesystem_capacity, container_filesystem_usage, container_memory_available, 
            container_memory_major_page_faults, container_memory_page_faults, container_memory_rss, 
            container_memory_usage, container_memory_working_set, k8s_pod_cpu_time, 
            k8s_pod_cpu_utilization, k8s_pod_filesystem_available, k8s_pod_filesystem_capacity, 
            k8s_pod_filesystem_usage, k8s_pod_memory_available, k8s_pod_memory_major_page_faults, 
            k8s_pod_memory_page_faults, k8s_pod_memory_rss, k8s_pod_memory_usage, 
            k8s_pod_memory_working_set, k8s_pod_network_errors, k8s_pod_network_io, 
            k8s_volume_available, k8s_volume_capacity, k8s_volume_inodes, 
            k8s_volume_inodes_free, k8s_volume_inodes_used, k8s_container_cpu_limit, 
            k8s_container_cpu_request, k8s_container_memory_limit, 
            k8s_container_memory_request, k8s_container_ready, k8s_container_restarts, 
            k8s_pod_phase
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
