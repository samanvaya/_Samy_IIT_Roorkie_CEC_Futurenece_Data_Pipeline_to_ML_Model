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

## Step 11 — business questions

The executable notebook is [step11_business_questions_test.ipynb](./step11_business_questions_test.ipynb). The SQL query files are [business_questions_etl.sql](./include/sql/business_questions_etl.sql) and [business_questions_elt.sql](./include/sql/business_questions_elt.sql).

The notebook answers these questions for both pipelines:

1. Daily net revenue by zone and store.
2. Revenue and gross margin by category.
3. Average delivery time and percentage within 15 minutes.
4. Revenue by payment mode and unreconciled amount.
5. Top 10 customers by spend and loyalty tier.
6. Rejected rows by file and failure reason.

It also compares like-for-like September delivered net revenue. If totals differ, it lists the exact order IDs included by only one pipeline so the difference can be explained from data, rather than reported as an unexplained mismatch. Results are exported to `assignment/step11_business_questions/` when the notebook is run.

## Step 12 — governance documentation

### Dataset ownership

| Dataset | Business owner | Technical custodian | Change approval |
|---|---|---|---|
| Orders and delivery facts | Operations and Finance | Data Engineering | Operations + Finance for business rules; Data Engineering for implementation |
| Payments and reconciliation | Finance | Data Engineering | Finance approves payment and revenue definitions |
| Customers | CRM / Customer Operations | Data Engineering | CRM and Compliance approve personal-data use |
| Products | Merchandising | Data Engineering | Merchandising approves catalogue and pricing rules |
| Stores and capacity | Operations | Data Engineering | Operations approves store and capacity rules |
| Raw, staging, marts and quarantine | Data Engineering, with Finance/Compliance access as needed | Data Engineering | Data Governance Council approves retention and access changes |

The 120-minute delivery limit and other quality rules require Operations and Finance approval because they affect service reporting and revenue. Data Engineering owns the implementation, tests, and deployment.

### Data dictionary

| Table / column | Type | Meaning and source | Allowed values / rule | PII |
|---|---|---|---|---|
| `etl_fact_orders.order_id` | string | Order key from `orders.csv` | Unique and non-empty | No |
| `etl_fact_orders.order_date` | date | Date derived from `orders.order_ts` | Valid timestamp | No |
| `etl_fact_orders.net_revenue` | decimal | Delivered `order_amount` from orders | Zero for non-delivered orders | No |
| `etl_fact_orders.gross_margin` | decimal | Net revenue less quantity × product cost | Derived finance metric | No |
| `etl_dim_customer.customer_id` | string | Customer key from `customers.csv` | Must exist in orders | No |
| `etl_dim_customer.full_name` | string | Customer name from customers | Restricted reporting field | Yes: indirect identifier |
| `etl_dim_customer.email_masked` | string | Masked customer email | Masked before reporting | Yes: derived from PII |
| `etl_dim_customer.phone_masked` | string | Masked customer phone | Masked before reporting | Yes: derived from PII |
| `etl_dim_customer.loyalty_tier` | string | Loyalty tier from customers | SILVER, GOLD, PLATINUM, or source-approved value | No |
| `etl_dim_product.category` | string | Product category from products | Non-empty catalogue category | No |
| `etl_dim_product.cost_price` | decimal | Product cost from products | Positive and not greater than MRP | No |
| `etl_dim_store.zone` | string | Store zone from stores | North, South, East, or West | No |
| `etl_payment_reconciliation.reason` | string | Reason an order and payment do not reconcile | Controlled reason text | No |
| `dq_results.status` | string | Result of a quality rule | PASS or FAIL | No |
| `dq_results.severity` | string | Impact of a quality failure | Critical or Warning | No |

Raw tables retain source columns as text plus `_loaded_at`, `_source_file`, and `_run_id`. ELT staging casts types, masks customer email and phone, and adds `dq_status` and `failure_reason`. The mart tables contain business-ready aggregates and masked customer attributes only.

### Data lineage

The lineage for September daily net revenue is:

`include/data/nammamart/orders.csv` → ETL extract/quality checks → `etl_fact_orders` → `etl_daily_zone_revenue`

or:

`include/data/nammamart/orders.csv` → `raw_orders` → `stg_orders` → `mart_daily_store_kpis`.

Products and stores are joined through their corresponding dimensions. The ETL quarantine files and `dq_results` explain rejected source rows. ELT is easier to replay because raw source values remain available with their source file and run ID.

### Personal-data handling

Customer `full_name`, `email`, `phone`, `city`, `area`, and address-related `pincode` are treated as personal or potentially identifying data. Raw customer data is retained only for approved audit users. ETL creates `email_masked` and `phone_masked`; ELT creates masked fields in staging and marts. Masking preserves analytical joins and customer segmentation without exposing direct contact details. Raw PII must not be copied into logs, screenshots, marts, or notebooks.

The applicable Indian framework is the Digital Personal Data Protection Act, 2023. NammaMart should document purpose, access, retention, security safeguards, and deletion/withdrawal handling, and should process only the personal data needed for the stated business purpose.

### Access control

- Raw tables and raw customer files: restricted to Data Engineering, authorised CRM, Finance, and Compliance users.
- Quarantine: restricted to Data Engineering, Operations data stewards, and Compliance investigators.
- Staging: analysts may access masked fields; direct raw PII remains restricted.
- Marts and business reports: Finance, Operations, and approved analysts receive least-privilege read access.
- Airflow administration and warehouse write access: limited to Data Engineering service identities.

### Retention

Raw records should be retained for the approved audit and replay period, for example seven years if Finance and Compliance approve that period. Quarantine records should be retained until the incident is resolved plus the incident-retention period. Masked marts should follow the analytics retention policy. After expiry, records must be deleted or irreversibly anonymised by a logged, access-controlled retention job. Retention periods must be confirmed with Legal and Compliance before production use.

### Secrets management

No API keys, Slack webhook URLs, passwords, tokens, or credentials belong in this repository. The repository contains only code that reads values from Airflow Connections/Variables or environment variables, such as `AZURE_OPENAI_API_KEY` and `SLACK_WEBHOOK_URL`. Values are configured locally or in the deployment secret store, excluded by `.gitignore`, and never printed in logs. If a secret is exposed, revoke and rotate it immediately and inspect Git history.

### Auditability

ETL records `run_id`, `dag_id`, timing, extracted rows, loaded rows, quarantined rows, and status in `pipeline_runs`; it writes critical rejected records with `failure_reason` and stores rule-level results in `dq_results`. ELT preserves every raw source row with `_source_file`, `_loaded_at`, and `_run_id`, then records row-level `dq_status` and `failure_reason` in staging. Finance can select a rejected `order_id` from quarantine, show the original raw row, identify the failed rule, and verify that it is absent from `etl_fact_orders` and finance revenue totals.

### Change history and returned orders

The October increment contains both 150 new orders and 10 corrections. ETL upserts by `order_id`, so a September order corrected to `RETURNED` is reflected in the current fact view and no longer contributes delivered revenue. ELT appends the increment to raw storage and uses the latest row per `order_id` in staging, preserving both the original and corrected versions for audit while presenting the latest state in marts.

Finance needs both views: a current operational view reflecting the return and an immutable historical/as-of view showing what was reported before the correction. Keep raw ELT versions and add effective timestamps or an SCD/event-history table before production so either view can be reconstructed.

### Repository safety rules

Never commit API keys, Slack webhook URLs, passwords, other credentials, or unmasked customer data outside the permitted input-data folder. The governance validation notebook is [step12_governance_documentation_test.ipynb](./step12_governance_documentation_test.ipynb); it checks this document, scans text/config files for secret-shaped values, and reports customer-data files outside the approved input paths.

## Step 13 — recommendation

The one-page CTO recommendation is [RECOMMENDATION_TO_CTO.md](./RECOMMENDATION_TO_CTO.md), and its validation notebook is [step13_recommendation_test.ipynb](./step13_recommendation_test.ipynb).

The recommendation is to adopt **ELT as the primary warehouse architecture**, with a certified finance-facing contract that publishes only approved rows and metrics. It compares auditability, data-quality guarantees, rule-change effort, performance at 1,000× volume, cost, and operational risks using the observed NammaMart results. It ends with exactly three risks: incorrect `dq_status` filtering, warehouse cost/latency growth, and overexposure of raw PII.
