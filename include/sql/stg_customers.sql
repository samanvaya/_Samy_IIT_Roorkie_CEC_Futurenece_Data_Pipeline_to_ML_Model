-- Cast customer fields, mask PII and retain a row-level quality outcome.
CREATE OR REPLACE TABLE stg_customers AS
SELECT
    customer_id,
    full_name,
    CASE WHEN regexp_matches(email, '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')
         THEN substr(email, 1, 2) || '***@' || split_part(email, '@', 2)
         ELSE '***' END AS email_masked,
    CASE WHEN length(regexp_replace(phone, '[^0-9]', '', 'g')) >= 4
         THEN substr(regexp_replace(phone, '[^0-9]', '', 'g'), 1, 2)
              || repeat('*', greatest(length(regexp_replace(phone, '[^0-9]', '', 'g')) - 4, 0))
              || right(regexp_replace(phone, '[^0-9]', '', 'g'), 2)
         ELSE '***' END AS phone_masked,
    city,
    area,
    pincode,
    try_cast(signup_date AS DATE) AS signup_date,
    upper(loyalty_tier) AS loyalty_tier,
    is_active,
    CASE WHEN customer_id IS NOT NULL AND customer_id <> '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    CASE WHEN customer_id IS NOT NULL AND customer_id <> '' THEN NULL ELSE 'Missing customer_id' END AS failure_reason
FROM raw_customers
QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY _loaded_at) = 1;
