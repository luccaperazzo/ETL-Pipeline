import os  # acceso a variables de entorno y operaciones de ruta
from datetime import timedelta  # para configurar retrasos/retries
from airflow import DAG  # clase DAG de Airflow
from airflow.operators.python import PythonOperator  # operador para tareas Python
from airflow.utils.dates import days_ago  # helper para fechas de inicio

# Importamos la función principal del ETL local al paquete `etl`
from etl.etl import run_etl
from sqlalchemy import create_engine, text  # para validación de carga contra Postgres

# URI de la base a la que se cargan los datos (se puede sobreescribir con env var DB_URI)
DB_URI = os.environ.get("DB_URI", "postgresql+psycopg2://postgres:postgres@postgres:5432/salesdb")
# Directorio donde se esperan los CSV diarios (puede cambiarse desde env DATA_DIR)
DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/dags/data")
# Esquema de BD opcional (env DB_SCHEMA)
SCHEMA = os.environ.get("DB_SCHEMA", None)

# Argumentos por defecto para las tareas del DAG
default_args = {
    "owner": "airflow",  # responsable mostrado en la UI
    "depends_on_past": False,  # no depender de ejecuciones previas
    "retries": 2,  # número de reintentos en caso de fallo
    "retry_delay": timedelta(minutes=5),  # espera entre reintentos
}

__version__ = "1.0.0"  # versión del DAG

with DAG(
    dag_id="sales_daily_etl",  # identificador del DAG
    default_args=default_args,  # aplica `default_args` a las tareas
    description="Daily ETL for sales CSV files into Postgres",  # descripción legible
    schedule_interval="@daily",  # programación diaria
    start_date=days_ago(1),  # fecha de inicio para la planificación
    catchup=False,  # no ejecutar cargas atrasadas al arrancar
    max_active_runs=1,  # limitar ejecuciones simultáneas
) as dag:

    # Documentación del DAG visible en la UI (incluye versión)
    dag.doc_md = """
    ## Sales daily ETL (version {ver})

    This DAG runs once per day and performs the following tasks:
    1. Check existence of the daily CSV file named `YYYY-MM-DD.csv` in `DATA_DIR`.
    2. Run ETL implemented in `airflow/dags/etl/etl.py` which validates, transforms and upserts rows into `sales_orders`.
    3. Validate the load by counting rows inserted for that date.

    Requirements:
    - Set environment variable `DB_URI` pointing to a Postgres database using SQLAlchemy URI (e.g. `postgresql+psycopg2://user:pass@host:5432/dbname`).
    - Set `DATA_DIR` to the directory containing the CSV files (defaults to `/opt/airflow/dags/data`).

    Idempotency is provided by the ETL using `ON CONFLICT (order_id)` upserts.
    """.format(ver=__version__)

    # --- tareas auxiliares definidas como funciones Python ---
    def check_file(ds, **context):
        # `ds` es la fecha de ejecución en formato YYYY-MM-DD (cadena) pasada por Airflow
        file_path = os.path.join(DATA_DIR, f"{ds}.csv")  # ruta esperada del CSV
        if not os.path.exists(file_path):  # si el archivo no existe
            # lanzamos excepción para que la tarea falle y se reintente según `retries`
            raise FileNotFoundError(f"Expected file not found: {file_path}")
        return file_path  # devolver ruta para registros/logs

    def run_etl_task(ds, **context):
        # Construye la ruta del CSV y delega en la función `run_etl` del módulo ETL
        file_path = os.path.join(DATA_DIR, f"{ds}.csv")
        return run_etl(file_path, DB_URI, schema=SCHEMA)

    def validate_load(ds, **context):
        # Validación simple: contar filas en `sales_orders` para la fecha de ejecución
        engine = create_engine(DB_URI)  # crear engine SQLAlchemy
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM sales_orders WHERE date = :d"), {"d": ds})
            count = result.scalar()  # obtener el conteo simple
        if count is None:
            # Si la query no devolvió nada, señalamos error
            raise ValueError("Validation query returned no result")
        return int(count)


    # --- definición de tareas/operadores ---
    t_check = PythonOperator(task_id="check_file_exists", python_callable=check_file, provide_context=True)
    t_etl = PythonOperator(task_id="run_etl", python_callable=run_etl_task, provide_context=True)
    t_validate = PythonOperator(task_id="validate_load", python_callable=validate_load, provide_context=True)

    # Definición del orden de ejecución: check -> etl -> validate
    t_check >> t_etl >> t_validate
