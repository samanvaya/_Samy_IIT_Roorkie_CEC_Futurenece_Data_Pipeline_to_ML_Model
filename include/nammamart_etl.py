"""
NammaMart ETL implementation shared by the Airflow DAG and local notebook.

The module keeps orchestration in the DAG and keeps business logic here. That
separation lets us test the complete ETL flow locally before running it inside
Docker/Airflow.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from data_quality import run_quality_checks
from masking import mask_email, mask_phone


DATA_DIR = Path(__file__).resolve().parent / "data" / "nammamart"
DEFAULT_WAREHOUSE = Path(__file__).resolve().parent / "warehouse_samy.duckdb"
DEFAULT_QUARANTINE_DIR = Path(__file__).resolve().parent / "quarantine" / "samy"

SOURCE_FILES = {
    "orders": "orders.csv",
    "orders_increment": "orders_2026-10-01_increment.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}


def load_source_data(data_dir: str | Path = DATA_DIR) -> dict[str, pd.DataFrame]:
    """Read all six source files without changing the source values."""

    source_dir = Path(data_dir)
    # Reading is isolated in one function so the DAG and notebook use exactly
    # the same source-file mapping and cannot accidentally load different data.
    return {
        name: pd.read_csv(source_dir / filename)
        for name, filename in SOURCE_FILES.items()
    }


def _source_file_name(dataset_name: str) -> str:
    """Convert an internal dataset name into the name used in issue logs."""

    return SOURCE_FILES[dataset_name]


def validate_data(
    data: dict[str, pd.DataFrame],
    *,
    run_id: str,
    dag_id: str = "nammamart_etl_samy",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run Step 4 checks and return the issue log plus the DQ scorecard."""

    # The shared validator records run metadata so the same scorecard can be
    # written to DuckDB by the Airflow version of this pipeline.
    return run_quality_checks(data, run_id=run_id, dag_id=dag_id)


def quarantine_critical_rows(
    data: dict[str, pd.DataFrame],
    issues: pd.DataFrame,
    *,
    run_id: str,
    quarantine_dir: str | Path = DEFAULT_QUARANTINE_DIR,
) -> list[Path]:
    """Write critical failures with one combined ``failure_reason`` per row."""

    output_dir = Path(quarantine_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    critical = issues[issues["severity"] == "Critical"].copy()
    written_files: list[Path] = []

    if critical.empty:
        return written_files

    # Combine multiple failed checks for the same source row into one reason,
    # which makes the quarantine file useful to an operations user.
    reason_by_row = (
        critical.groupby(["file", "row_key"], dropna=False)["problem"]
        .apply(lambda values: "; ".join(dict.fromkeys(values)))
        .reset_index(name="failure_reason")
    )

    for dataset_name, frame in data.items():
        file_name = _source_file_name(dataset_name)
        file_reasons = reason_by_row[reason_by_row["file"] == file_name]
        if file_reasons.empty:
            continue

        # Convert keys to strings for a stable join even when pandas inferred
        # one source column as numeric in a malformed input file.
        source = frame.copy()
        source["_join_key"] = source["order_id" if "order_id" in source.columns else (
            "payment_id" if "payment_id" in source.columns else (
                "customer_id" if "customer_id" in source.columns else (
                    "product_id" if "product_id" in source.columns else "store_id"
                )
            )
        )].astype(str)
        file_reasons = file_reasons.copy()
        file_reasons["_join_key"] = file_reasons["row_key"].astype(str)
        failed_rows = source.merge(file_reasons[["_join_key", "failure_reason"]], on="_join_key", how="inner")
        failed_rows = failed_rows.drop(columns=["_join_key"])
        path = output_dir / f"{Path(file_name).stem}_failed_{run_id}.csv"
        failed_rows.to_csv(path, index=False)
        written_files.append(path)

    return written_files


def _critical_keys(issues: pd.DataFrame, file_name: str) -> set[str]:
    """Return source keys that must not enter the clean reporting table."""

    selected = issues[(issues["file"] == file_name) & (issues["severity"] == "Critical")]
    return set(selected["row_key"].astype(str))


def _remove_critical_rows(frame: pd.DataFrame, key: str, blocked_keys: set[str]) -> pd.DataFrame:
    """Filter critical records while preserving the source DataFrame columns."""

    return frame[~frame[key].astype(str).isin(blocked_keys)].copy()


def build_clean_tables(data: dict[str, pd.DataFrame], issues: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Transform valid data into the ETL star-schema inputs."""

    # Critical order failures are excluded from finance facts. Warning-level
    # PII format issues remain loadable and are protected during masking.
    orders = _remove_critical_rows(data["orders"], "order_id", _critical_keys(issues, "orders.csv"))

    # Dimension duplicates are reduced to one deterministic row so joins do
    # not multiply revenue. Their original failures remain in quarantine/logs.
    customers = data["customers"].drop_duplicates("customer_id", keep="first").copy()
    products = data["products"].drop_duplicates("product_id", keep="first").copy()
    stores = data["stores"].drop_duplicates("store_id", keep="first").copy()

    # Remove dimensions with critical completeness/accuracy issues before the
    # join; any order depending on them will fail the inner join and stay out
    # of the finance fact table.
    customers = _remove_critical_rows(customers, "customer_id", _critical_keys(issues, "customers.csv"))
    products = _remove_critical_rows(products, "product_id", _critical_keys(issues, "products.csv"))
    stores = _remove_critical_rows(stores, "store_id", _critical_keys(issues, "stores.csv"))

    customers["email_masked"] = customers["email"].map(mask_email)
    customers["phone_masked"] = customers["phone"].map(mask_phone)

    # Inner joins enforce referential integrity in the final star schema.
    fact = orders.merge(customers, on="customer_id", how="inner", suffixes=("", "_customer"))
    fact = fact.merge(products, on="product_id", how="inner", suffixes=("", "_product"))
    fact = fact.merge(stores, on="store_id", how="inner", suffixes=("", "_store"))

    fact["order_ts"] = pd.to_datetime(fact["order_ts"], errors="coerce")
    fact["order_date"] = fact["order_ts"].dt.date.astype(str)
    fact["net_revenue"] = fact["order_amount"].where(fact["order_status"] == "DELIVERED", 0.0)
    fact["gross_margin"] = fact["net_revenue"] - (fact["quantity"] * fact["cost_price"])
    fact["within_15_min"] = (fact["order_status"] == "DELIVERED") & (fact["delivery_minutes"] <= 15)

    customer_dim = customers[["customer_id", "full_name", "email_masked", "phone_masked", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"]].copy()
    product_dim = products[["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"]].copy()
    store_dim = stores[["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"]].copy()

    return {
        "fact_orders": fact,
        "dim_customer": customer_dim,
        "dim_product": product_dim,
        "dim_store": store_dim,
    }


def build_payment_reconciliation(
    clean_tables: dict[str, pd.DataFrame],
    payments: pd.DataFrame,
) -> pd.DataFrame:
    """Create one reconciliation row for every payment/order mismatch."""

    fact = clean_tables["fact_orders"]
    order_amounts = fact[["order_id", "order_amount", "payment_mode", "order_status"]].drop_duplicates("order_id")
    joined = order_amounts.merge(payments, on="order_id", how="outer", indicator=True, suffixes=("_order", "_payment"))
    mismatch = joined[\
        (joined["_merge"] != "both")
        | ((joined["payment_status"] == "SUCCESS") & ((joined["amount_paid"] - joined["order_amount"]).abs() > 0.01))
        | (joined["payment_mode_order"].notna() & joined["payment_mode_payment"].notna() & (joined["payment_mode_order"] != joined["payment_mode_payment"]))
    ].copy()

    def reason(row: pd.Series) -> str:
        if row["_merge"] == "left_only":
            return "Missing payment for clean order"
        if row["_merge"] == "right_only":
            return "Payment references an order outside the clean fact table"
        if row["payment_status"] == "SUCCESS" and abs(row["amount_paid"] - row["order_amount"]) > 0.01:
            return "Successful payment amount does not match order amount"
        return "Payment mode does not match order payment mode"

    if mismatch.empty:
        return pd.DataFrame(columns=["order_id", "payment_id", "reason", "order_amount", "amount_paid"])
    mismatch["reason"] = mismatch.apply(reason, axis=1)
    return mismatch[["order_id", "payment_id", "reason", "order_amount", "amount_paid"]]


def build_reports(fact_orders: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build the first two business reports from the clean fact table."""

    daily_zone = (
        fact_orders[fact_orders["order_status"] == "DELIVERED"]
        .groupby(["order_date", "zone", "store_id"], as_index=False)["net_revenue"]
        .sum()
        .rename(columns={"net_revenue": "daily_net_revenue"})
    )
    category_margin = (
        fact_orders[fact_orders["order_status"] == "DELIVERED"]
        .groupby("category", as_index=False)
        .agg(revenue=("net_revenue", "sum"), gross_margin=("gross_margin", "sum"))
    )
    return {
        "daily_zone_revenue": daily_zone,
        "category_margin": category_margin,
    }


def write_warehouse(
    clean_tables: dict[str, pd.DataFrame],
    reconciliation: pd.DataFrame,
    reports: dict[str, pd.DataFrame],
    dq_results: pd.DataFrame,
    *,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
) -> None:
    """Write the ETL outputs idempotently by replacing each derived table."""

    warehouse = Path(warehouse_path)
    warehouse.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))
    try:
        frames = {
            "etl_fact_orders": clean_tables["fact_orders"],
            "etl_dim_customer": clean_tables["dim_customer"],
            "etl_dim_product": clean_tables["dim_product"],
            "etl_dim_store": clean_tables["dim_store"],
            "etl_payment_reconciliation": reconciliation,
            "etl_daily_zone_revenue": reports["daily_zone_revenue"],
            "etl_category_margin": reports["category_margin"],
            "dq_results": dq_results,
        }
        for table_name, frame in frames.items():
            # Replacing derived tables makes a rerun deterministic and avoids
            # double-counting revenue when the same source is processed twice.
            con.register("current_frame", frame)
            con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM current_frame')
            con.unregister("current_frame")
    finally:
        con.close()


def run_etl(
    *,
    data_dir: str | Path = DATA_DIR,
    warehouse_path: str | Path = DEFAULT_WAREHOUSE,
    quarantine_dir: str | Path = DEFAULT_QUARANTINE_DIR,
    run_id: str | None = None,
    dag_id: str = "nammamart_etl_samy",
    include_increment: bool = False,
) -> dict[str, Any]:
    """Run the ETL flow, optionally applying the next-day increment feed."""

    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    data = load_source_data(data_dir)
    increment_inserted = 0
    increment_updated = 0
    if include_increment:
        # The increment contains corrections and new records. Keeping the
        # latest row for each order_id implements an ETL upsert before quality
        # checks, so corrected orders are not misclassified as duplicates.
        original_order_ids = set(data["orders"]["order_id"].astype(str))
        increment = data["orders_increment"].copy()
        increment_ids = set(increment["order_id"].astype(str))
        increment_updated = len(original_order_ids & increment_ids)
        increment_inserted = len(increment_ids - original_order_ids)
        data["orders"] = (
            pd.concat([data["orders"], increment], ignore_index=True)
            .drop_duplicates("order_id", keep="last")
            .reset_index(drop=True)
        )

    issues, dq_results = validate_data(data, run_id=run_id, dag_id=dag_id)
    quarantine_files = quarantine_critical_rows(data, issues, run_id=run_id, quarantine_dir=quarantine_dir)
    clean_tables = build_clean_tables(data, issues)
    reconciliation = build_payment_reconciliation(clean_tables, data["payments"])
    reports = build_reports(clean_tables["fact_orders"])
    write_warehouse(clean_tables, reconciliation, reports, dq_results, warehouse_path=warehouse_path)

    return {
        "run_id": run_id,
        "rows_extracted": sum(len(frame) for frame in data.values()),
        "rows_loaded": len(clean_tables["fact_orders"]),
        "rows_quarantined": len(issues[issues["severity"] == "Critical"]),
        "issue_rows": len(issues),
        "quarantine_files": [str(path) for path in quarantine_files],
        "warehouse_path": str(warehouse_path),
        "reconciliation_rows": len(reconciliation),
        "reports": reports,
        "increment_inserted": increment_inserted,
        "increment_updated": increment_updated,
    }
