-- Step 11 business questions for the ELT staging and mart layers.
-- The marts are built from SQL-only transformations over raw/staging tables.

-- QUESTION: q1_daily_revenue_by_zone_store
SELECT order_date, zone, store_id, daily_net_revenue, delivered_orders
FROM mart_daily_store_kpis
WHERE order_date >= '2026-09-01' AND order_date < '2026-10-01'
ORDER BY order_date, zone, store_id;

-- QUESTION: q2_revenue_margin_by_category
SELECT category, revenue, gross_margin
FROM mart_category_margin
ORDER BY revenue DESC;

-- QUESTION: q3_delivery_sla
SELECT store_id, average_delivery_minutes, pct_within_15_min, delivered_orders
FROM mart_delivery_sla
ORDER BY store_id;

-- QUESTION: q4_payment_mode_reconciliation
WITH revenue AS (
    SELECT payment_mode, SUM(net_revenue) AS revenue
    FROM stg_orders
    WHERE dq_status = 'PASS'
      AND order_status = 'DELIVERED'
      AND order_date >= '2026-09-01' AND order_date < '2026-10-01'
    GROUP BY payment_mode
), mismatches AS (
    SELECT COALESCE(order_payment_mode, payment_payment_mode) AS payment_mode,
           SUM(COALESCE(amount_paid, 0) - COALESCE(order_amount, 0)) AS unreconciled_amount,
           COUNT(*) AS unreconciled_rows
    FROM mart_payment_reconciliation
    GROUP BY COALESCE(order_payment_mode, payment_payment_mode)
)
SELECT COALESCE(revenue.payment_mode, mismatches.payment_mode) AS payment_mode,
       COALESCE(revenue.revenue, 0) AS revenue,
       COALESCE(mismatches.unreconciled_amount, 0) AS unreconciled_amount,
       COALESCE(mismatches.unreconciled_rows, 0) AS unreconciled_rows
FROM revenue
FULL OUTER JOIN mismatches ON revenue.payment_mode = mismatches.payment_mode
ORDER BY payment_mode;

-- QUESTION: q5_top_customers
SELECT customer_id, full_name, loyalty_tier, total_spend
FROM mart_top_customers
ORDER BY total_spend DESC
LIMIT 10;

-- QUESTION: q6_rejected_rows
WITH rejected AS (
    SELECT 'orders.csv' AS file_name, failure_reason, COUNT(*) AS rejected_rows
    FROM stg_orders
    WHERE dq_status = 'FAIL' AND failure_reason IS NOT NULL AND failure_reason <> ''
    GROUP BY failure_reason
    UNION ALL
    SELECT 'payments.csv', failure_reason, COUNT(*)
    FROM stg_payments
    WHERE dq_status = 'FAIL' AND failure_reason IS NOT NULL AND failure_reason <> ''
    GROUP BY failure_reason
    UNION ALL
    SELECT 'customers.csv', failure_reason, COUNT(*)
    FROM stg_customers
    WHERE dq_status = 'FAIL' AND failure_reason IS NOT NULL AND failure_reason <> ''
    GROUP BY failure_reason
    UNION ALL
    SELECT 'products.csv', failure_reason, COUNT(*)
    FROM stg_products
    WHERE dq_status = 'FAIL' AND failure_reason IS NOT NULL AND failure_reason <> ''
    GROUP BY failure_reason
    UNION ALL
    SELECT 'stores.csv', failure_reason, COUNT(*)
    FROM stg_stores
    WHERE dq_status = 'FAIL' AND failure_reason IS NOT NULL AND failure_reason <> ''
    GROUP BY failure_reason
)
SELECT file_name, failure_reason, rejected_rows
FROM rejected
ORDER BY file_name, rejected_rows DESC;
