"""Airflow DAG wrapper for the NammaMart ETL pipeline."""

from datetime import timedelta
import re
import sys

import duckdb
import pandas as pd
import pendulum
from airflow.sdk import dag, get_current_context, task

sys.path.insert(0, "/usr/local/airflow/include")

from nammamart_etl import (
    DATA_DIR,
    DEFAULT_QUARANTINE_DIR,
    DEFAULT_WAREHOUSE,
    SOURCE_FILES,
    add_dimension_failures,
    apply_increment,
    build_clean_tables,
    build_payment_reconciliation,
    build_reports,
    load_source_data,
    load_source_file,
    quarantine_critical_rows,
    record_dq_results,
    rejected_order_count,
    validate_data,
    write_tables,
)
from observability import record_pipeline_run

DAG_ID = "nammamart_etl_samy"


def _run_id() -> str:
    """Airflow's run_id, made safe for quarantine file names."""

    return re.sub(r"[^A-Za-z0-9_-]", "_", get_current_context()["run_id"])


def _source_data() -> tuple[dict[str, pd.DataFrame], int]:
    """All six files (increment upserted when present) and the rows read."""

    data = load_source_data(DATA_DIR)
    rows_read = sum(len(frame) for frame in data.values())
    if (DATA_DIR / SOURCE_FILES["orders_increment"]).exists():
        data, _, _ = apply_increment(data)
    return data, rows_read


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
    """Define Extract → Validate → Transform/Load → Reconcile → Report → Audit."""

    # One extract per source file, run in parallel. Each reads only its own
    # file, so a missing file fails exactly one task in the Graph view.
    @task
    def extract(dataset_name: str) -> int:
        rows = len(load_source_file(dataset_name, DATA_DIR))
        print(f"[Extract] {SOURCE_FILES[dataset_name]}: {rows} rows")
        return rows

    @task
    def validate_and_quarantine(extract_counts: list[int]) -> dict:
        """Validate all files, quarantine Critical rows and store dq_results."""

        run_id = _run_id()
        data, rows_read = _source_data()
        issues, dq_results = validate_data(data, run_id=run_id, dag_id=DAG_ID)
        issues = add_dimension_failures(data, issues)
        quarantine_files = quarantine_critical_rows(
            data,
            issues,
            run_id=run_id,
            quarantine_dir=DEFAULT_QUARANTINE_DIR,
        )
        scorecard = record_dq_results(
            dq_results,
            order_rows=len(data["orders"]),
            run_id=run_id,
            dag_id=DAG_ID,
            warehouse_path=DEFAULT_WAREHOUSE,
        )
        critical = issues[issues["severity"] == "Critical"]
        print(f"[Validate] {len(scorecard)} checks, {int((scorecard['status'] == 'FAIL').sum())} failed")
        print(critical.groupby(["file", "problem"]).size().to_string())
        # JSON keeps the small issue log portable through XCom while the
        # source data itself stays on disk.
        return {
            "run_id": run_id,
            "rows_extracted": rows_read,
            "rows_quarantined": rejected_order_count(issues),
            "issues": issues.to_json(orient="records"),
            "quarantine_files": [str(path) for path in quarantine_files],
        }

    @task
    def transform_and_load(validation: dict) -> dict:
        """Standardise, dedupe, mask and join; replace the star-schema tables."""

        issues = pd.read_json(validation["issues"], orient="records")
        clean_tables = build_clean_tables(_source_data()[0], issues)
        write_tables(
            {
                "etl_fact_orders": clean_tables["fact_orders"],
                "etl_dim_customer": clean_tables["dim_customer"],
                "etl_dim_product": clean_tables["dim_product"],
                "etl_dim_store": clean_tables["dim_store"],
            },
            warehouse_path=DEFAULT_WAREHOUSE,
        )
        rows_loaded = len(clean_tables["fact_orders"])
        print(f"[Load] etl_fact_orders: {rows_loaded} rows")
        return {"rows_loaded": rows_loaded}

    @task
    def reconcile_payments(_loaded: dict) -> int:
        """Compare clean orders with payments.csv; one row per mismatch."""

        con = duckdb.connect(str(DEFAULT_WAREHOUSE), read_only=True)
        try:
            fact = con.execute("SELECT * FROM etl_fact_orders").fetchdf()
        finally:
            con.close()
        reconciliation = build_payment_reconciliation({"fact_orders": fact}, load_source_file("payments", DATA_DIR))
        write_tables({"etl_payment_reconciliation": reconciliation}, warehouse_path=DEFAULT_WAREHOUSE)
        print(reconciliation.groupby("reason").size().to_string())
        return len(reconciliation)

    @task
    def report(_reconciliation_rows: int) -> dict:
        """Build the two report tables and print them in the task log."""

        con = duckdb.connect(str(DEFAULT_WAREHOUSE), read_only=True)
        try:
            fact = con.execute("SELECT * FROM etl_fact_orders").fetchdf()
        finally:
            con.close()
        reports = build_reports(fact)
        write_tables(
            {
                "etl_daily_zone_revenue": reports["daily_zone_revenue"],
                "etl_category_margin": reports["category_margin"],
            },
            warehouse_path=DEFAULT_WAREHOUSE,
        )
        with pd.option_context("display.max_rows", 20, "display.width", 120):
            print("[Report] etl_daily_zone_revenue\n", reports["daily_zone_revenue"])
            print("[Report] etl_category_margin\n", reports["category_margin"])
        return {name: len(frame) for name, frame in reports.items()}

    @task(trigger_rule="all_done")
    def audit_completion() -> None:
        """Write one pipeline_runs row whether the run succeeded or failed."""

        context = get_current_context()
        ti = context["ti"]
        validation = ti.xcom_pull(task_ids="validate_and_quarantine") or {}
        loaded = ti.xcom_pull(task_ids="transform_and_load") or {}
        reported = ti.xcom_pull(task_ids="report")
        # report is the last business task: no result means something upstream failed.
        status = "SUCCESS" if reported is not None else "FAILED"
        start = getattr(context.get("dag_run"), "start_date", None) or pendulum.now("UTC")
        record_pipeline_run(
            DEFAULT_WAREHOUSE,
            run_id=_run_id(),
            dag_id=DAG_ID,
            start_time=pendulum.instance(start).to_iso8601_string(),
            end_time=pendulum.now("UTC").to_iso8601_string(),
            rows_extracted=validation.get("rows_extracted", 0),
            rows_loaded=loaded.get("rows_loaded", 0),
            rows_quarantined=validation.get("rows_quarantined", 0),
            status=status,
        )
        print(f"[Audit] {DAG_ID} run {_run_id()} finished with status {status}")

    extracted = [
        extract.override(task_id=f"extract_{name}")(name)
        for name in ["orders", "payments", "customers", "products", "stores"]
    ]
    validation = validate_and_quarantine(extracted)
    loaded = transform_and_load(validation)
    reported = report(reconcile_payments(loaded))
    audit = audit_completion()
    reported >> audit


nammamart_etl_samy()
