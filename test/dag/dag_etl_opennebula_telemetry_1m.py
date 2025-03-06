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
    'db_table': "metrics_edge_1m",
    
    'bucket': "metrics",
    'window_period': "1m",
    'default_start_time': datetime(1970, 1, 1, tzinfo=pytz.UTC)
}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 20, tzinfo=pytz.UTC), #datetime.combine(datetime.now().date(), time(8, 0)),  # Empieza a las 8:00 del día actual
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #'end_date': datetime.combine(datetime.now().date(), time(18, 0)),  # Termina a las 18:00 del día actual
}

dag = DAG(
    'sync_etl_opennebula_telemetry_1m',
    default_args=default_args,
    description='Sync Opennebula Telemetry 1m',
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
        service_id VARCHAR NOT NULL,
        opennebula_libvirt_memory_available_bytes FLOAT,
        opennebula_libvirt_memory_maximum_bytes FLOAT,
        opennebula_libvirt_cpu_seconds_total FLOAT,
        opennebula_libvirt_cpu_system_seconds_total FLOAT,
        opennebula_libvirt_cpu_user_seconds_total FLOAT,
        PRIMARY KEY (_time, service_id)
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
                r["_measurement"] == "opennebula_libvirt_memory_available_bytes" or
                r["_measurement"] == "opennebula_libvirt_memory_maximum_bytes" or
                r["_measurement"] == "opennebula_libvirt_cpu_seconds_total" or
                r["_measurement"] == "opennebula_libvirt_cpu_system_seconds_total" or
                r["_measurement"] == "opennebula_libvirt_cpu_user_seconds_total" 
             )
            |> filter(fn: (r) => r["_field"] == "gauge")
            |> aggregateWindow(every: {config['window_period']}, fn: mean, createEmpty: false)
            |> yield(name: "mean")
            '''
    
    client = InfluxDBClient(url=config['influxdb_url'],token=config['token'],org=config['org'])
    query_api = client.query_api()
    result = query_api.query_data_frame(query=query, org=config['org'])
    #result = pd.concat(result, ignore_index=True)
    client.close()

    # Guardar el resultado en XComs
    ti.xcom_push(key='raw_data', value=result)

# Tarea de transformación
def transform(ti):
    print("Transforming data")
    # Recoger los datos del paso anterior
    raw_data = ti.xcom_pull(key='raw_data', task_ids='ingest_task')
    
    #get columns
    columns = ['_time','one_vm_id','_measurement','_value']
    raw_data = raw_data[columns]
    
    # Pivotar el DataFrame
    pivot_df = raw_data.pivot_table(index=['_time', 'one_vm_id'], columns='_measurement', values='_value').reset_index()
    
    # Convertir la columna _time a tipo datetime
    pivot_df['_time'] = pd.to_datetime(pivot_df['_time'])

    # Filtrar los resultados que tengan intervalos de 1 minuto exactos
    filtered_df = pivot_df[(pivot_df['_time'].dt.second == 0)].copy()

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
        service_id = row['one_vm_id']
        opennebula_libvirt_memory_available_bytes = row['opennebula_libvirt_memory_available_bytes'] 
        opennebula_libvirt_memory_maximum_bytes = row['opennebula_libvirt_memory_maximum_bytes'] 
        opennebula_libvirt_cpu_seconds_total = row['opennebula_libvirt_cpu_seconds_total'] 
        opennebula_libvirt_cpu_system_seconds_total = row['opennebula_libvirt_cpu_system_seconds_total'] 
        opennebula_libvirt_cpu_user_seconds_total = row['opennebula_libvirt_cpu_user_seconds_total'] 
    
    
        # Insertar datos en PostgreSQL
        insert_query = f'''
            INSERT INTO {config['db_table']} (
                _time,
                service_id,
                opennebula_libvirt_memory_available_bytes,
                opennebula_libvirt_memory_maximum_bytes,
                opennebula_libvirt_cpu_seconds_total,
                opennebula_libvirt_cpu_system_seconds_total,
                opennebula_libvirt_cpu_user_seconds_total
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (_time,service_id) DO NOTHING;
                '''
        pg_cursor.execute(insert_query, (
            _time,
            service_id,
            opennebula_libvirt_memory_available_bytes,
            opennebula_libvirt_memory_maximum_bytes,
            opennebula_libvirt_cpu_seconds_total,
            opennebula_libvirt_cpu_system_seconds_total,
            opennebula_libvirt_cpu_user_seconds_total
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
