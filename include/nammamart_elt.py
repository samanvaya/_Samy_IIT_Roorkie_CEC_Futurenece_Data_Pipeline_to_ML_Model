"""NammaMart ELT implementation: raw load in Python, transformations in SQL."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from nammamart_etl import DATA_DIR, SOURCE_FILES


SQL_DIR = Path(__file__).resolve().parent / "sql"
DEFAULT_WAREHOUSE = Path(__file__).resolve().parent / "warehouse_samy_elt.duckdb"

RAW_TABLES = {
    "orders": "raw_orders",
    "payments": "raw_payments",
    "customers": "raw_customers",
    "products": "raw_products",
    "stores": "raw_stores",
}

STAGING_SQL_FILES = [
    "stg_customers.sql",
    "stg_products.sql",
    "stg_stores.sql",
    "stg_orders.sql",
    "stg_payments.sql",
]

MART_SQL_FILES = [
    "mart_daily_store_kpis.sql",
    "mart_category_margin.sql",
    "mart_delivery_sla.sql",
    "mart_payment_reconciliation.sql",
    "mart_top_customers.sql",
]


def load_raw_tables(
    *,
    data_dir: str | Path = DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    run_id: str = "local-elt-run",
) -> None:
    """Load the five base files as text, adding only ingestion metadata."""

    source_dir = Path(data_dir)
    con = duckdb.connect(str(warehouse_path))
    loaded_at = datetime.now(timezone.utc).isoformat()
    try:
        for dataset_name, table_name in RAW_TABLES.items():
            # ``keep_default_na=False`` preserves empty source fields as empty
            # strings rather than silently converting them to pandas NaN.
            frame = pd.read_csv(
                source_dir / SOURCE_FILES[dataset_name],
                dtype=str,
                keep_default_na=False,
            )
            frame["_loaded_at"] = loaded_at
            frame["_source_file"] = SOURCE_FILES[dataset_name]
            frame["_run_id"] = run_id
            con.register("raw_frame", frame)
            con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM raw_frame')
            con.unregister("raw_frame")
    finally:
        con.close()


def execute_sql_layers(*, warehouse_path: str | Path = DEFAULT_WAREHOUSE) -> None:
    """Run every staging and mart transformation from a checked-in SQL file."""

    con = duckdb.connect(str(warehouse_path))
    try:
        # SQL files are executed in a fixed order so marts always see fully
        # created staging tables and are reproducible across reruns.
        for filename in STAGING_SQL_FILES + MART_SQL_FILES:
            con.execute((SQL_DIR / filename).read_text())
    finally:
        con.close()


def run_elt(
    *,
    data_dir: str | Path = DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    run_id: str = "local-elt-run",
) -> dict[str, int | str]:
    """Run the raw, staging and mart layers and return table metrics."""

    load_raw_tables(data_dir=data_dir, warehouse_path=warehouse_path, run_id=run_id)
    execute_sql_layers(warehouse_path=warehouse_path)
    con = duckdb.connect(str(warehouse_path), read_only=True)
    try:
        metrics = {
            "warehouse_path": str(warehouse_path),
            "raw_orders": con.execute("SELECT count(*) FROM raw_orders").fetchone()[0],
            "stg_orders": con.execute("SELECT count(*) FROM stg_orders").fetchone()[0],
            "passing_orders": con.execute("SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS'").fetchone()[0],
            "mart_store_rows": con.execute("SELECT count(*) FROM mart_daily_store_kpis").fetchone()[0],
            "mart_category_rows": con.execute("SELECT count(*) FROM mart_category_margin").fetchone()[0],
            "reconciliation_rows": con.execute("SELECT count(*) FROM mart_payment_reconciliation").fetchone()[0],
        }
    finally:
        con.close()
    return metrics
