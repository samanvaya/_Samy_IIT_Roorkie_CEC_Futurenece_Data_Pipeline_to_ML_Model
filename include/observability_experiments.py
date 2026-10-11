"""Repeatable Step 10 observability experiments for both NammaMart pipelines.

Every experiment works on a temporary copy of the source directory.  This is
deliberate: the original CSV files are never edited, so restoring the source
after each experiment is automatic even when a test raises an exception.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import shutil
import tempfile
from typing import Callable, Iterator

import pandas as pd

from nammamart_etl import DATA_DIR, run_etl
from nammamart_elt import run_elt, run_incremental_elt


REPO_ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def isolated_source_copy() -> Iterator[Path]:
    """Yield a disposable source copy and remove it when the test finishes."""

    with tempfile.TemporaryDirectory(prefix="nammamart_step10_") as temp_dir:
        source_copy = Path(temp_dir) / "nammamart"
        # copytree gives each experiment its own independent source files.
        shutil.copytree(DATA_DIR, source_copy)
        yield source_copy
    # TemporaryDirectory removes the altered copy; the repository source is
    # therefore restored automatically after both successful and failed tests.


def _result(experiment: int, dag: str, expected: str, observed: str, change: str) -> dict[str, str | int]:
    """Create one report row using the assignment's required four fields."""

    return {
        "experiment": experiment,
        "dag": dag,
        "expected": expected,
        "observed": observed,
        "where_seen": "Local pipeline return value, DuckDB audit/quality tables, or captured exception",
        "what_to_change": change,
    }


def _run_etl(source_dir: Path, work_dir: Path, tag: str, **kwargs):
    """Run the same ETL implementation imported by ``nammamart_etl_samy``."""

    return run_etl(
        data_dir=source_dir,
        warehouse_path=work_dir / f"{tag}_etl.duckdb",
        quarantine_dir=work_dir / f"{tag}_etl_quarantine",
        run_id=f"step10-{tag}-etl",
        **kwargs,
    )


def _run_elt(source_dir: Path, work_dir: Path, tag: str):
    """Run the same ELT implementation imported by ``nammamart_elt_samy``."""

    return run_elt(
        data_dir=source_dir,
        warehouse_path=work_dir / f"{tag}_elt.duckdb",
        run_id=f"step10-{tag}-elt",
    )


def _capture(function: Callable[[], object]) -> tuple[object | None, str | None]:
    """Return a function result or a readable exception without stopping the suite."""

    try:
        return function(), None
    except Exception as error:  # noqa: BLE001 - experiments must record failures.
        return None, f"{type(error).__name__}: {error}"


def _normal_run(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        etl, etl_error = _capture(lambda: _run_etl(source, work_dir, "exp01"))
        elt, elt_error = _capture(lambda: _run_elt(source, work_dir, "exp01"))
        expected = "Both DAG implementations complete and publish observable row counts."
        return [
            _result(1, "ETL", expected, str(etl if etl_error is None else etl_error), "Keep the happy-path audit and row-count checks."),
            _result(1, "ELT", expected, str(elt if elt_error is None else elt_error), "Keep the happy-path audit and row-count checks."),
        ]


def _idempotency(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        etl_db = work_dir / "exp02_etl.duckdb"
        elt_db = work_dir / "exp02_elt.duckdb"
        first_etl = run_etl(data_dir=source, warehouse_path=etl_db, quarantine_dir=work_dir / "exp02_etl_q", run_id="step10-exp02-etl-1")
        second_etl = run_etl(data_dir=source, warehouse_path=etl_db, quarantine_dir=work_dir / "exp02_etl_q", run_id="step10-exp02-etl-2")
        first_elt = run_elt(data_dir=source, warehouse_path=elt_db, run_id="step10-exp02-elt-1")
        second_elt = run_elt(data_dir=source, warehouse_path=elt_db, run_id="step10-exp02-elt-2")
        return [
            _result(2, "ETL", "Fact row count does not change after a rerun.", f"{first_etl['rows_loaded']} -> {second_etl['rows_loaded']}", "Keep replace/upsert semantics and add an automated row-count assertion."),
            _result(2, "ELT", "Raw and staging counts remain stable for a base-data rerun.", f"{first_elt['raw_orders']} -> {second_elt['raw_orders']}; staging {first_elt['stg_orders']} -> {second_elt['stg_orders']}", "Keep raw replacement for a rerun and use append only for the increment feed."),
        ]


def _incremental(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        etl = _run_etl(source, work_dir, "exp03", include_increment=True)
        elt_db = work_dir / "exp03_elt.duckdb"
        run_elt(data_dir=source, warehouse_path=elt_db, run_id="step10-exp03-elt-base")
        elt = run_incremental_elt(data_dir=source, warehouse_path=elt_db, run_id="step10-exp03-elt-increment")
        expected = "150 new orders inserted, 10 corrected orders updated, and no duplicate staged order IDs."
        return [
            _result(3, "ETL", expected, f"inserted={etl['increment_inserted']}, updated={etl['increment_updated']}, fact_rows={etl['rows_loaded']}", "Keep the order_id upsert and monitor inserted/updated counts."),
            _result(3, "ELT", expected, f"inserted={elt['increment_inserted']}, updated={elt['increment_updated']}, raw_rows={elt['raw_orders']}, staged_rows={elt['stg_orders']}", "Keep append-only raw storage and latest-row SQL upsert."),
        ]


def _missing_file(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        (source / "products.csv").unlink()
        etl, etl_error = _capture(lambda: _run_etl(source, work_dir, "exp04"))
        elt, elt_error = _capture(lambda: _run_elt(source, work_dir, "exp04"))
        expected = "Both pipelines fail loudly on the missing products file; Airflow retries and marks the source task failed."
        return [
            _result(4, "ETL", expected, etl_error or str(etl), "Add a source-file preflight check and alert on the failed extract task."),
            _result(4, "ELT", expected, elt_error or str(elt), "Add the same preflight check before raw loading."),
        ]


def _write_five_invalid_rows(source: Path) -> None:
    """Append one row for each required invalid-data case to a disposable copy."""

    orders = pd.read_csv(source / "orders.csv")
    template = orders.iloc[0].copy()
    invalid_rows = []
    for suffix, column, value in [
        ("NULL_CUSTOMER", "customer_id", ""),
        ("NEGATIVE_QTY", "quantity", -1),
        ("UNKNOWN_STORE", "store_id", "STORE-UNKNOWN"),
        ("BAD_STATUS", "order_status", "SHIPPED"),
        ("FUTURE_DATE", "order_ts", "2027-01-01 10:00:00"),
    ]:
        row = template.copy()
        row["order_id"] = f"STEP10-{suffix}"
        row[column] = value
        invalid_rows.append(row)
    pd.concat([orders, pd.DataFrame(invalid_rows)], ignore_index=True).to_csv(source / "orders.csv", index=False)


def _invalid_data(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        _write_five_invalid_rows(source)
        etl = _run_etl(source, work_dir, "exp05")
        elt = _run_elt(source, work_dir, "exp05")
        expected = "The five added rows are rejected or marked FAIL with the matching reasons."
        return [
            _result(5, "ETL", expected, f"quarantined={etl['rows_quarantined']}, issue_rows={etl['issue_rows']}", "Keep row-level reasons and add assertions for every injected defect."),
            _result(5, "ELT", expected, f"passing_staging={elt['passing_orders']}, failed_staging={elt['stg_orders'] - elt['passing_orders']}", "Expose failure_reason and dq_status in the staging audit query."),
        ]


def _schema_change(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        orders = pd.read_csv(source / "orders.csv").rename(columns={"order_amount": "total"})
        orders.to_csv(source / "orders.csv", index=False)
        etl, etl_error = _capture(lambda: _run_etl(source, work_dir, "exp06"))
        elt, elt_error = _capture(lambda: _run_elt(source, work_dir, "exp06"))
        expected = "Both pipelines fail loudly because the required order_amount column is absent."
        return [
            _result(6, "ETL", expected, etl_error or str(etl), "Add an explicit schema contract before validation."),
            _result(6, "ELT", expected, elt_error or str(elt), "Add SQL schema checks and alert on the failed transform task."),
        ]


def _volume_anomaly(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        orders = pd.read_csv(source / "orders.csv").head(50)
        orders.to_csv(source / "orders.csv", index=False)
        etl = _run_etl(source, work_dir, "exp07")
        elt = _run_elt(source, work_dir, "exp07")
        expected = "A greater-than-90% volume drop is detected and reported."
        observed = "Pipeline completed; current implementation has no explicit volume threshold check."
        return [
            _result(7, "ETL", expected, f"{observed} loaded={etl['rows_loaded']}", "Add a run-over-run volume check to dq_results and alert when volume drops below 10%."),
            _result(7, "ELT", expected, f"{observed} passing_staging={elt['passing_orders']}", "Add a SQL volume check and persist it in the observability scorecard."),
        ]


def _distribution_drift(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        orders = pd.read_csv(source / "orders.csv")
        store = orders.iloc[0]["store_id"]
        selected = orders["store_id"] == store
        orders.loc[selected, "unit_price"] = orders.loc[selected, "unit_price"] * 100
        orders.to_csv(source / "orders.csv", index=False)
        etl = _run_etl(source, work_dir, "exp08")
        elt = _run_elt(source, work_dir, "exp08")
        expected = "The price/amount consistency rule catches affected rows and the store metric changes."
        return [
            _result(8, "ETL", expected, f"quarantined={etl['rows_quarantined']}, affected_store={store}", "Add a distribution baseline so extreme price shifts are caught even when amount is updated too."),
            _result(8, "ELT", expected, f"failed_staging={elt['stg_orders'] - elt['passing_orders']}, affected_store={store}", "Add SQL distribution thresholds and trend them by store."),
        ]


def _stale_data(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        orders = pd.read_csv(source / "orders.csv")
        orders["order_ts"] = (pd.Timestamp.utcnow() - pd.Timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        orders.to_csv(source / "orders.csv", index=False)
        etl = _run_etl(source, work_dir, "exp09")
        elt = _run_elt(source, work_dir, "exp09")
        expected = "A timeliness check warns that the latest order is 30 days old."
        observed = "Pipeline completes; current implementation checks future dates but does not check stale latest-order age."
        return [
            _result(9, "ETL", expected, f"{observed} loaded={etl['rows_loaded']}", "Add a latest-order freshness warning to dq_results."),
            _result(9, "ELT", expected, f"{observed} passing_staging={elt['passing_orders']}", "Add a SQL freshness query and alert when the latest order is older than one day."),
        ]


def _recovery(work_dir: Path) -> list[dict[str, str | int]]:
    with isolated_source_copy() as source:
        (source / "products.csv").unlink()
        _, etl_failure = _capture(lambda: _run_etl(source, work_dir, "exp10"))
        _, elt_failure = _capture(lambda: _run_elt(source, work_dir, "exp10"))
        # Restore only the disposable copy, then rerun downstream work. The
        # real repository files were never modified at any point.
        shutil.copy(DATA_DIR / "products.csv", source / "products.csv")
        etl_success, etl_error = _capture(lambda: _run_etl(source, work_dir, "exp10-recovered"))
        elt_success, elt_error = _capture(lambda: _run_elt(source, work_dir, "exp10-recovered"))
        return [
            _result(10, "ETL", "After fixing the source, clearing the failed task reruns it and downstream tasks successfully.", f"failure={etl_failure}; recovery={etl_success or etl_error}", "In Airflow, use Clear on the failed extract and downstream tasks; keep the audit row for the failed attempt."),
            _result(10, "ELT", "After fixing the source, clearing the failed load/transform path reruns successfully.", f"failure={elt_failure}; recovery={elt_success or elt_error}", "In Airflow, clear only the failed task and its downstream dependants, then verify pipeline_runs."),
        ]


def run_all_experiments() -> pd.DataFrame:
    """Run all ten experiments for both pipelines and return the report."""

    with tempfile.TemporaryDirectory(prefix="nammamart_step10_work_") as work:
        work_dir = Path(work)
        rows: list[dict[str, str | int]] = []
        for experiment in (_normal_run, _idempotency, _incremental, _missing_file, _invalid_data, _schema_change, _volume_anomaly, _distribution_drift, _stale_data, _recovery):
            rows.extend(experiment(work_dir))
    return pd.DataFrame(rows, columns=["experiment", "dag", "expected", "observed", "where_seen", "what_to_change"])


def write_report(report: pd.DataFrame, output_dir: str | Path = REPO_ROOT / "assignment") -> tuple[Path, Path]:
    """Persist CSV and Markdown copies of the experiment results."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "step10_observability_experiments.csv"
    md_path = output_dir / "step10_observability_experiments.md"
    report.to_csv(csv_path, index=False)
    md_path.write_text(
        "# Step 10 observability experiments\n\n"
        "The report was generated using isolated temporary source copies. Original files were not modified.\n\n"
        + report.to_markdown(index=False)
        + "\n\n## Observability pillars\n\n"
        "- Freshness: currently weakest; add a latest-order-age check.\n"
        "- Volume: currently weakest alongside freshness; add a run-over-run threshold.\n"
        "- Schema: covered by loud missing-column failures; add a formal contract.\n"
        "- Distribution: partially covered by amount consistency; add baselines by store.\n"
        "- Lineage: strongest because ETL quarantine and ELT raw tables preserve traceability.\n"
    )
    return csv_path, md_path
