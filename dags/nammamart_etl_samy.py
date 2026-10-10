"""Airflow DAG wrapper for the NammaMart ETL pipeline."""

from datetime import timedelta
import sys

import pandas as pd
import pendulum
from airflow.decorators import dag, task

sys.path.insert(0, "/usr/local/airflow/include")

from nammamart_etl import (
    DATA_DIR,
    DEFAULT_QUARANTINE_DIR,
    DEFAULT_WAREHOUSE,
    build_clean_tables,
    build_payment_reconciliation,
    build_reports,
    load_source_data,
    quarantine_critical_rows,
    validate_data,
    write_warehouse,
)


@dag(
    dag_id="nammamart_etl_samy",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        "owner": "samy",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["nammamart", "etl", "assignment"],
)
def nammamart_etl_samy():
    """Define Extract → Validate → Transform → Load → Report dependencies."""

    # Each extract is separate so Airflow can run the five source reads in
    # parallel and show a clear source-level failure in the Graph view.
    @task
    def extract_orders() -> str:
        load_source_data(DATA_DIR)["orders"]
        return "orders.csv"

    @task
    def extract_payments() -> str:
        load_source_data(DATA_DIR)["payments"]
        return "payments.csv"

    @task
    def extract_customers() -> str:
        load_source_data(DATA_DIR)["customers"]
        return "customers.csv"

    @task
    def extract_products() -> str:
        load_source_data(DATA_DIR)["products"]
        return "products.csv"

    @task
    def extract_stores() -> str:
        load_source_data(DATA_DIR)["stores"]
        return "stores.csv"

    @task
    def validate_and_quarantine(_source_files: list[str]) -> dict:
        """Validate all source files and persist critical rows for review."""

        run_id = pendulum.now("UTC").strftime("%Y%m%dT%H%M%SZ")
        data = load_source_data(DATA_DIR)
        issues, dq_results = validate_data(data, run_id=run_id)
        quarantine_files = quarantine_critical_rows(
            data,
            issues,
            run_id=run_id,
            quarantine_dir=DEFAULT_QUARANTINE_DIR,
        )
        # JSON keeps the small validation result portable through Airflow's
        # XCom mechanism while the original source data remains on disk.
        return {
            "run_id": run_id,
            "issues": issues.to_json(orient="records"),
            "dq_results": dq_results.to_json(orient="records"),
            "quarantine_files": [str(path) for path in quarantine_files],
        }

    @task
    def transform_and_load(validation_result: dict) -> dict:
        """Transform only accepted rows and replace the derived DuckDB tables."""

        data = load_source_data(DATA_DIR)
        issues = pd.read_json(validation_result["issues"], orient="records")
        dq_results = pd.read_json(validation_result["dq_results"], orient="records")
        clean_tables = build_clean_tables(data, issues)
        reconciliation = build_payment_reconciliation(clean_tables, data["payments"])
        reports = build_reports(clean_tables["fact_orders"])
        write_warehouse(
            clean_tables,
            reconciliation,
            reports,
            dq_results,
            warehouse_path=DEFAULT_WAREHOUSE,
        )
        return {
            "run_id": validation_result["run_id"],
            "rows_loaded": len(clean_tables["fact_orders"]),
            "reconciliation_rows": len(reconciliation),
        }

    @task
    def report(result: dict) -> None:
        """Print the final ETL metrics so they are visible in Airflow logs."""

        print(f"[ETL] Run: {result['run_id']}")
        print(f"[ETL] Clean fact rows loaded: {result['rows_loaded']}")
        print(f"[ETL] Payment reconciliation rows: {result['reconciliation_rows']}")

    extracted = [
        extract_orders(),
        extract_payments(),
        extract_customers(),
        extract_products(),
        extract_stores(),
    ]
    validation = validate_and_quarantine(extracted)
    loaded = transform_and_load(validation)
    report(loaded)


nammamart_etl_samy()
