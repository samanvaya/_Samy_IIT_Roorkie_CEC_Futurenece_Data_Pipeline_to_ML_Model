"""Airflow DAG wrapper for the NammaMart SQL-based ELT pipeline."""

from datetime import timedelta
import sys

import pendulum
from airflow.decorators import dag, task

sys.path.insert(0, "/usr/local/airflow/include")

from nammamart_elt import execute_sql_layers, load_raw_tables


@dag(
    dag_id="nammamart_elt_samy",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args={
        "owner": "samy",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["nammamart", "elt", "assignment"],
)
def nammamart_elt_samy():
    """Define Extract → Load raw → Transform SQL → Build marts."""

    @task
    def load_raw() -> str:
        # Raw ingestion deliberately performs no pandas cleaning; source
        # values are retained as text for audit and replay.
        load_raw_tables(run_id=pendulum.now("UTC").strftime("%Y%m%dT%H%M%SZ"))
        return "raw tables loaded"

    @task
    def transform_sql(_raw_status: str) -> str:
        # All business transformations are stored in include/sql and executed
        # in order so the Airflow task remains a transparent SQL runner.
        execute_sql_layers()
        return "staging and marts built"

    transform_sql(load_raw())


nammamart_elt_samy()
