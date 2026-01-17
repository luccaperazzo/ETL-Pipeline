Resumen

Este documento describe el flujo del pipeline ETL diario que ingiere CSVs, los transforma, deduplica por `order_id` y carga los datos idempotentemente en PostgreSQL. El pipeline está orquestado por Apache Airflow y se puede ejecutar localmente con Docker Compose.

Componentes

- Orquestador: Apache Airflow (imagen oficial, modo `standalone`).
- Transformación: Python + pandas (funciones en `airflow/dags/etl/etl.py`).
- Almacenamiento: PostgreSQL (tabla `sales_orders` creada con `sql/create_table.sql`).
- Inicialización DB: `scripts/init_db.py` aplica el DDL y crea la base si falta.
- DAG: `airflow/dags/sales_etl_dag.py` (tareas: verificar archivo, ejecutar ETL, validar carga).

Flujo diario (alto nivel)

1. Airflow evalúa el DAG programado (daily).
2. Tarea `check_file_exists` verifica que el CSV para la fecha de ejecución exista en `dags/data/YYYY-MM-DD.csv`.
   - Si no existe, la tarea actualmente marca retry (se puede cambiar a ShortCircuit/FileSensor para evitar deadlocks).
3. Tarea `run_etl` llama a la función `run_etl(file_path, db_uri, schema)` de `etl/etl.py`:
   - Lee el CSV con `pandas.read_csv()`.
   - Ejecuta `validate_and_transform()` (normaliza columnas, valida, convierte tipos, descarta filas inválidas y deduplica por `order_id`, conservando la fila más reciente).
   - Inserta/actualiza filas en Postgres usando `INSERT ... ON CONFLICT (order_id) DO UPDATE` para garantizar idempotencia.
4. Tarea `validate_load` consulta la base y compara conteos/condiciones para validar la carga.
5. El DAG finaliza con estado SUCCESS si todas las tareas finalizan correctamente.

Idempotencia y deduplicación

- La tabla `sales_orders` tiene `order_id` como clave primaria.
- El upsert usa `ON CONFLICT` para que ejecutar el mismo CSV múltiples veces no cree duplicados.
- `validate_and_transform()` elimina duplicados en el archivo por `order_id` (se conserva la fila con la fecha más reciente antes del upsert).

Ejecución local (resumen rápido)

- Levantar Postgres / Airflow con Docker Compose (usar `docker-compose-airflow.yml`).
- Inicializar DB y esquema:

  - Ejecutar `python scripts/init_db.py --db-uri "postgresql://..."` si es necesario.

- Probar ETL standalone:

  - `python airflow/dags/etl/etl.py data/2025-07-01.csv "postgresql://..."`

- Desde Airflow UI: despausar DAG `sales_daily_etl` y trigger manual.

Ubicaciones clave en el repo

- DAG: [airflow/dags/sales_etl_dag.py](airflow/dags/sales_etl_dag.py)
- ETL core: [airflow/dags/etl/etl.py](airflow/dags/etl/etl.py)
- SQL DDL: [sql/create_table.sql](sql/create_table.sql)
- DB init: [scripts/init_db.py](scripts/init_db.py)
- Sample data: [airflow/dags/data/2025-07-01.csv](airflow/dags/data/2025-07-01.csv)

Resolución de bloqueos (lección aprendida)

- Si `check_file_exists` lanza FileNotFoundError y hay retries simultáneos, el scheduler puede reportar "task deadlock" si no hay tareas ejecutables debido a límites de concurrencia.
- Mitigación rápida: colocar el CSV esperado en `dags/data/` (se hizo en la prueba). Solución robusta: reemplazar la excepción por ShortCircuitOperator o FileSensor con timeout y manejar con `sla`/logs.

Mejoras sugeridas

- Mover `DB_URI` a una Connection de Airflow en vez de usar env var para mayor seguridad.
- Reemplazar la verificación de archivo por `FileSensor` o `ShortCircuitOperator` con timeout y buena señalización (skip en vez de retry).
- Añadir alertas/SLAs y timeouts en tareas largas.
- Guardar logs y métricas de filas procesadas en una tabla de auditoría.

Si querés, actualizo el DAG para tolerar archivos faltantes (ShortCircuit/FileSensor) ahora.