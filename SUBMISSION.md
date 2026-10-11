# NammaMart Step 10 — observability experiments

The executable test notebook is [step10_observability_experiments_test.ipynb](./step10_observability_experiments_test.ipynb). It runs all ten required experiments against both pipeline implementations and produces 20 report rows: one ETL row and one ELT row per experiment.

## Required experiment matrix

| # | Experiment | Expected evidence | Change recorded by the notebook |
|---:|---|---|---|
| 1 | Normal successful run | Both implementations complete; row counts, quarantine counts, and audit records are visible | Keep run audit and row-count checks |
| 2 | Re-run for idempotency | ETL fact rows and ELT base raw/staging rows remain stable | Keep replacement/upsert semantics and add row-count assertions |
| 3 | Incremental load | 150 inserts, 10 updates, no duplicate staged order IDs | Monitor inserted/updated counts |
| 4 | Missing source file | Both fail loudly; Airflow shows retries and a failed source task | Add source-file preflight and alerting |
| 5 | New invalid data | Five injected defects receive matching quarantine or `dq_status` reasons | Assert every injected defect is recorded |
| 6 | Schema change | Missing `order_amount` fails loudly instead of silently loading incorrect data | Add a formal schema contract |
| 7 | Volume anomaly | A greater-than-90% drop is detected | Add a run-over-run volume check |
| 8 | Distribution drift | Price/amount consistency catches the drift; store metrics change | Add store-level distribution baselines |
| 9 | Stale data | A 30-day-old latest order raises a freshness warning | Add latest-order-age checks |
| 10 | Recovery after failure | After fixing the file, clearing failed/downstream Airflow tasks succeeds | Verify `pipeline_runs` retains the failed and recovered attempts |

## Restoration guarantee

Each experiment runs inside `TemporaryDirectory` after copying the six source files. Missing files and modified rows are applied only to that temporary copy. Cleanup runs when the experiment exits, including when a test raises an exception, so the original files are restored automatically.

## Airflow-specific evidence

The notebook tests the same business-logic modules imported by the DAGs. Experiments 4, 6, and 10 additionally require Airflow UI/log evidence for retry states, task failures, and Clear-based recovery. Add those screenshots after running [nammamart_etl_samy.py](./dags/nammamart_etl_samy.py) and [nammamart_elt_samy.py](./dags/nammamart_elt_samy.py).

## Observability pillars

- **Freshness:** weakest until the latest-order-age check is added.
- **Volume:** requires an explicit run-over-run threshold check.
- **Schema:** missing required columns fail loudly; a formal contract is recommended.
- **Distribution:** partially covered by amount consistency; store-level baselines are recommended.
- **Lineage:** strongest because ETL quarantine and ELT raw tables preserve traceability.
