-- One row per order/payment mismatch, including missing or extra payments.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH joined AS (
    SELECT
        o.order_id,
        p.payment_id,
        o.order_amount,
        p.amount_paid,
        o.payment_mode AS order_payment_mode,
        p.payment_mode AS payment_payment_mode,
        p.payment_status,
        CASE
            WHEN p.order_id IS NULL THEN 'Missing payment for order'
            WHEN o.order_id IS NULL THEN 'Payment references order outside passing staging rows'
            WHEN p.payment_status = 'SUCCESS' AND abs(p.amount_paid - o.order_amount) > 0.01 THEN 'Successful payment amount mismatch'
            WHEN o.payment_mode <> p.payment_mode THEN 'Payment mode mismatch'
            ELSE NULL
        END AS reason
    FROM stg_orders o
    FULL OUTER JOIN stg_payments p ON o.order_id = p.order_id
    WHERE o.dq_status = 'PASS' OR p.dq_status = 'PASS'
)
SELECT * FROM joined WHERE reason IS NOT NULL;
