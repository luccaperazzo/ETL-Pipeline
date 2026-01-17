import os
import sys
import logging
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

REQUIRED_COLS = ["order_id", "customer", "amount", "date"]


def validate_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: c.strip().lower())
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df[REQUIRED_COLS].copy()
    df["order_id"] = pd.to_numeric(df["order_id"], errors="coerce").astype("Int64")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    before = len(df)
    df = df.dropna(subset=["order_id", "amount", "date"]).copy()
    after = len(df)
    logger.info("Dropped %s invalid rows", before - after)

    df = df.sort_values("date").drop_duplicates(subset=["order_id"], keep="last")

    df["order_id"] = df["order_id"].astype(int)
    df["customer"] = df["customer"].astype(str)
    df["amount"] = df["amount"].astype(float)

    return df


def get_engine(db_uri: str) -> Engine:
    if not db_uri:
        raise ValueError("DB_URI not provided")
    return create_engine(db_uri)


def upsert_orders(engine: Engine, df: pd.DataFrame, schema: Optional[str] = None) -> int:
    if df.empty:
        logger.info("No rows to upsert")
        return 0

    rows = df.to_dict(orient="records")

    tbl = "sales_orders" if not schema else f"{schema}.sales_orders"
    insert_sql = (
        f"INSERT INTO {tbl} (order_id, customer, amount, date) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (order_id) DO UPDATE SET customer = EXCLUDED.customer, amount = EXCLUDED.amount, date = EXCLUDED.date"
    )

    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        args = [(r["order_id"], r["customer"], r["amount"], r["date"]) for r in rows]
        cur.executemany(insert_sql, args)
        conn.commit()
        cur.close()
        logger.info("Upserted %s rows", len(args))
        return len(args)
    finally:
        conn.close()


def run_etl(file_path: str, db_uri: str, schema: Optional[str] = None) -> int:
    logger.info("Running ETL for %s", file_path)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    df = pd.read_csv(file_path)
    df = validate_and_transform(df)

    engine = get_engine(db_uri)
    count = upsert_orders(engine, df, schema=schema)
    return count


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python etl.py <csv_path> <db_uri> [schema]")
        sys.exit(2)
    path = sys.argv[1]
    db_uri = sys.argv[2]
    schema = sys.argv[3] if len(sys.argv) > 3 else None
    try:
        count = run_etl(path, db_uri, schema)
        print(f"Upserted {count} rows")
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        raise
