**Pipeline ETL batch con Airflow + Pandas + PostgreSQL**

- **Arquitectura**: File System -> Airflow DAG -> Pandas ETL -> PostgreSQL

Requisitos locales:
- Python 3.9+
- PostgreSQL (puede ejecutarse con `docker-compose up -d postgres`)
- (Opcional) Apache Airflow instalado o ejecutar usando la imagen oficial de Airflow

Instalación de dependencias (entorno virtual recomendado):

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Inicializar base de datos (crear tabla):

```bash
# usando psql
psql postgresql://postgres:postgres@localhost:5432/salesdb -f sql/create_table.sql
```

Ejecutar ETL de prueba (sin Airflow):

```bash
python airflow/dags/etl/etl.py data/2025-07-01.csv postgresql+psycopg2://postgres:postgres@localhost:5432/salesdb
```

Usar con Airflow:
- Copiar el directorio `airflow/dags` al directorio `dags` de tu instalación de Airflow, o montar este repo en la imagen Docker de Airflow.
- Exportar variables de entorno: `DB_URI` (por ejemplo `postgresql+psycopg2://postgres:postgres@postgres:5432/salesdb`) y `DATA_DIR` (ruta donde Airflow encontrará `YYYY-MM-DD.csv`).
- Luego activar el DAG `sales_daily_etl` en la UI de Airflow.

Notas:
- El ETL es idempotente: usa `ON CONFLICT (order_id)` para evitar duplicados.
- El esquema de la tabla está en `sql/create_table.sql`.

**Ejecución completa con Docker (Airflow + Postgres)**

Se incluye un `docker-compose-airflow.yml` con un servicio `postgres` y una imagen oficial `apache/airflow`.

Pasos rápidos:

```powershell
docker compose -f docker-compose-airflow.yml up -d
# esperar a que Airflow esté listo, luego abrir http://localhost:8080
```

Notas:
- Monta los `dags` y la carpeta `data` en la imagen de Airflow (ya configurado en el compose).
- Por simplicidad el contenedor de Airflow usa `LocalExecutor` y la BD interna `airflow` en Postgres.

Variables importantes en el entorno del contenedor Airflow:
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (ya configurada en el compose para apuntar al Postgres del compose)
- Coloca tus CSVs en `data/` con nombres `YYYY-MM-DD.csv` para que el DAG los detecte.

Archivos clave:
- `airflow/dags/sales_etl_dag.py` - DAG de orquestación
- `airflow/dags/etl/etl.py` - script ETL (puede ejecutarse standalone)
- `sql/create_table.sql` - esquema DB
- `requirements.txt` - dependencias Python

