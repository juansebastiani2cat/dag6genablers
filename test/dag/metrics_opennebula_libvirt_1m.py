from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, time
import pandas as pd
import psycopg2
import pytz
from influxdb_client import InfluxDBClient
import re


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
    'db_table': "metrics_opennebula_libvirt_1m",
    
    'bucket': "metrics",
    'window_period': "1m",
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
    'metrics_opennebula_libvirt_1m',
    default_args=default_args,
    description='metrics opennebula libvirt 1m',
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
            one_vm_id VARCHAR NOT NULL,
            one_vm_worker VARCHAR NOT NULL,
            opennebula_libvirt_cpu_seconds_total FLOAT,
            opennebula_libvirt_cpu_system_seconds_total FLOAT,
            opennebula_libvirt_cpu_user_seconds_total FLOAT,
            opennebula_libvirt_vcpu_maximum FLOAT,
            opennebula_libvirt_vcpu_online FLOAT,
            opennebula_libvirt_vcpu_state FLOAT,
            opennebula_libvirt_vcpu_time_seconds_total FLOAT,
            opennebula_libvirt_vcpu_wait_seconds_total FLOAT,
            PRIMARY KEY (_time, one_vm_id)
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
                r["_measurement"] == "opennebula_libvirt_cpu_seconds_total" or
                r["_measurement"] == "opennebula_libvirt_cpu_system_seconds_total" or
                r["_measurement"] == "opennebula_libvirt_cpu_user_seconds_total" or
                r["_measurement"] == "opennebula_libvirt_vcpu_maximum" or
                r["_measurement"] == "opennebula_libvirt_vcpu_online" or
                r["_measurement"] == "opennebula_libvirt_vcpu_state" or
                r["_measurement"] == "opennebula_libvirt_vcpu_time_seconds_total" or
                r["_measurement"] == "opennebula_libvirt_vcpu_wait_seconds_total")
        |> filter(fn: (r) => r["_field"] == "gauge")
        |> aggregateWindow(every: {config['window_period']}, fn: mean, createEmpty: false)
        |> yield(name: "mean")
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
    
    # Función para normalizar el campo one_vm_name
    def normalize_name(name):
        # Usar una expresión regular para eliminar todo después del primer paréntesis
        return re.sub(r"_\(.*\)$", "", name)
    
    columns = ['_time','_measurement','_value','one_vm_name','one_vm_id']
    opennebula_libvirt = raw_data[columns]
    #aplico normalización
    opennebula_libvirt.loc[:, 'one_vm_name'] = opennebula_libvirt['one_vm_name'].apply(normalize_name)

    opennebula_libvirt_sorted = opennebula_libvirt.sort_values(by='_time', ascending=False)
    opennebula_libvirt_sorted = opennebula_libvirt_sorted.reset_index(drop=True)

    # Pivotar los datos e incluir `one_vm_name`
    pivot_opennebula_libvirt = opennebula_libvirt_sorted.pivot_table(index=['_time', 'one_vm_name', 'one_vm_id'], columns='_measurement', values='_value', dropna=False).reset_index()
    pivot_opennebula_libvirt = pivot_opennebula_libvirt.dropna()
    pivot_opennebula_libvirt =pivot_opennebula_libvirt.rename(columns={"one_vm_name": "one_vm_worker"})
    
    pivot_opennebula_libvirt['_time'] = pd.to_datetime(pivot_opennebula_libvirt['_time'])
    filtered_df = pivot_opennebula_libvirt[(pivot_opennebula_libvirt['_time'].dt.second == 0)].copy()
   
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
        one_vm_id = row['one_vm_id']
        one_vm_worker = row['one_vm_worker']
        opennebula_libvirt_cpu_seconds_total = row['opennebula_libvirt_cpu_seconds_total'] 
        opennebula_libvirt_cpu_system_seconds_total = row['opennebula_libvirt_cpu_system_seconds_total'] 
        opennebula_libvirt_cpu_user_seconds_total = row['opennebula_libvirt_cpu_user_seconds_total']
        opennebula_libvirt_vcpu_maximum = row['opennebula_libvirt_vcpu_maximum']
        opennebula_libvirt_vcpu_online = row['opennebula_libvirt_vcpu_online']
        opennebula_libvirt_vcpu_state = row['opennebula_libvirt_vcpu_state']
        opennebula_libvirt_vcpu_time_seconds_total = row['opennebula_libvirt_vcpu_time_seconds_total']
        opennebula_libvirt_vcpu_wait_seconds_total = row['opennebula_libvirt_vcpu_wait_seconds_total']


        # Insertar datos en PostgreSQL
        insert_query = f'''
            INSERT INTO {config['db_table']} (
                _time,
                one_vm_id,
                one_vm_worker,
                opennebula_libvirt_cpu_seconds_total,
                opennebula_libvirt_cpu_system_seconds_total,
                opennebula_libvirt_cpu_user_seconds_total,
                opennebula_libvirt_vcpu_maximum,
                opennebula_libvirt_vcpu_online,
                opennebula_libvirt_vcpu_state,
                opennebula_libvirt_vcpu_time_seconds_total,
                opennebula_libvirt_vcpu_wait_seconds_total
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (_time,one_vm_id) DO NOTHING;
            '''
        pg_cursor.execute(insert_query, (
            _time,
            one_vm_id,
            one_vm_worker,
            opennebula_libvirt_cpu_seconds_total,
            opennebula_libvirt_cpu_system_seconds_total,
            opennebula_libvirt_cpu_user_seconds_total,
            opennebula_libvirt_vcpu_maximum,
            opennebula_libvirt_vcpu_online,
            opennebula_libvirt_vcpu_state,
            opennebula_libvirt_vcpu_time_seconds_total,
            opennebula_libvirt_vcpu_wait_seconds_total
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
