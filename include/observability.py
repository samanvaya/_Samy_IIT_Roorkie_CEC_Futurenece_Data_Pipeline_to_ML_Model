"""Run-audit helpers shared by the ETL and ELT implementations."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


def record_pipeline_run(
    warehouse_path: str | Path,
    *,
    run_id: str,
    dag_id: str,
    start_time: str,
    end_time: str,
    rows_extracted: int,
    rows_loaded: int,
    rows_quarantined: int,
    status: str,
) -> None:
    """Append one externally visible audit record to ``pipeline_runs``."""

    # The audit table is created independently of the business tables, so a
    # failed pipeline can still leave behind a useful failure record.
    con = duckdb.connect(str(warehouse_path))
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR,
                dag_id VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_extracted BIGINT,
                rows_loaded BIGINT,
                rows_quarantined BIGINT,
                status VARCHAR
            )
            """
        )
        audit_row = pd.DataFrame(
            [{
                "run_id": run_id,
                "dag_id": dag_id,
                "start_time": start_time,
                "end_time": end_time,
                "rows_extracted": rows_extracted,
                "rows_loaded": rows_loaded,
                "rows_quarantined": rows_quarantined,
                "status": status,
            }]
        )
        con.register("audit_row", audit_row)
        con.execute("INSERT INTO pipeline_runs SELECT * FROM audit_row")
        con.unregister("audit_row")
    finally:
        con.close()
