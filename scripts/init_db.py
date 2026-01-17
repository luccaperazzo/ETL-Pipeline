import os
from sqlalchemy import create_engine, text
from urllib.parse import urlparse, urlunparse

DB_URI = os.environ.get("DB_URI", "postgresql+psycopg2://postgres:postgres@localhost:5432/salesdb")
SQL_FILE = os.path.join(os.path.dirname(__file__), "..", "sql", "create_table.sql")


def ensure_database_exists(db_uri: str):
    parsed = urlparse(db_uri)
    target_db = parsed.path.lstrip('/')
    if not target_db:
        return
    # build admin uri that connects to the default 'postgres' database
    admin_parsed = parsed._replace(path='/postgres')
    admin_uri = urlunparse(admin_parsed)
    admin_engine = create_engine(admin_uri, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        res = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :d"), {"d": target_db})
        exists = res.first() is not None
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{target_db}"'))
            print(f"Created database {target_db}")


if __name__ == "__main__":
    # ensure database exists, then apply schema
    ensure_database_exists(DB_URI)
    engine = create_engine(DB_URI)
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql = f.read()
    # use a transaction/commit for DDL
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("Schema applied")
