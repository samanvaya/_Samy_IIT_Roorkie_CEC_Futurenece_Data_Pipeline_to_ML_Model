-- Cast orders, standardise categorical values and calculate SQL quality flags.
CREATE OR REPLACE TABLE stg_orders AS
WITH typed AS (
    SELECT
        order_id,
        try_cast(order_ts AS TIMESTAMP) AS order_ts,
        customer_id,
        store_id,
        product_id,
        try_cast(quantity AS INTEGER) AS quantity,
        try_cast(unit_price AS DECIMAL(12, 2)) AS unit_price,
        try_cast(discount_pct AS DECIMAL(6, 2)) AS discount_pct,
        try_cast(order_amount AS DECIMAL(12, 2)) AS order_amount,
        CASE
            WHEN lower(trim(payment_mode)) = 'upi' THEN 'UPI'
            WHEN lower(trim(payment_mode)) IN ('card') THEN 'Card'
            WHEN lower(trim(payment_mode)) IN ('cod', 'cashh', 'cash on delivery') THEN 'Cash on Delivery'
            WHEN lower(trim(payment_mode)) = 'wallet' THEN 'Wallet'
            ELSE trim(payment_mode)
        END AS payment_mode,
        upper(trim(order_status)) AS order_status,
        channel,
        try_cast(delivery_minutes AS INTEGER) AS delivery_minutes,
        count(*) OVER (PARTITION BY order_id) AS order_id_count
    FROM raw_orders
), checked AS (
    SELECT *, round(quantity * unit_price * (1 - discount_pct / 100), 2) AS expected_amount
    FROM typed
)
SELECT
    *,
    CAST(order_ts AS DATE) AS order_date,
    CASE WHEN order_status = 'DELIVERED' THEN order_amount ELSE 0 END AS net_revenue,
    CASE WHEN order_id <> '' AND order_id_count = 1 AND customer_id <> '' AND store_id <> '' AND product_id <> ''
              AND quantity > 0 AND discount_pct BETWEEN 0 AND 100
              AND payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
              AND order_status IN ('DELIVERED', 'CANCELLED', 'RETURNED')
              AND abs(order_amount - expected_amount) <= 0.01
              AND (order_status <> 'DELIVERED' OR delivery_minutes IS NOT NULL)
              AND order_ts <= current_timestamp
              AND EXISTS (SELECT 1 FROM raw_customers c WHERE c.customer_id = checked.customer_id)
              AND EXISTS (SELECT 1 FROM raw_products p WHERE p.product_id = checked.product_id)
              AND EXISTS (SELECT 1 FROM raw_stores s WHERE s.store_id = checked.store_id)
         THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    concat_ws('; ',
        CASE WHEN order_id = '' THEN 'Missing order_id' END,
        CASE WHEN order_id_count > 1 THEN 'Duplicate order_id' END,
        CASE WHEN customer_id = '' THEN 'Missing customer_id' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM raw_customers c WHERE c.customer_id = checked.customer_id) THEN 'Unknown customer_id' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM raw_products p WHERE p.product_id = checked.product_id) THEN 'Unknown product_id' END,
        CASE WHEN NOT EXISTS (SELECT 1 FROM raw_stores s WHERE s.store_id = checked.store_id) THEN 'Unknown store_id' END,
        CASE WHEN quantity <= 0 THEN 'Quantity is not positive' END,
        CASE WHEN discount_pct NOT BETWEEN 0 AND 100 THEN 'Discount outside 0-100' END,
        CASE WHEN payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'Invalid payment mode' END,
        CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'Invalid order status' END,
        CASE WHEN abs(order_amount - expected_amount) > 0.01 THEN 'Amount does not match pricing formula' END,
        CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'Delivered order has no delivery time' END,
        CASE WHEN order_ts > current_timestamp THEN 'Future-dated order' END
    ) AS failure_reason
FROM checked;
