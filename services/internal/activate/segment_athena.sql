-- High-value customer segment on Athena (Trino SQL); same logic as query/segment.sql.
-- The FROM placeholders take the quoted database and table; ? are execution parameters:
-- window start date, window end date, minimum spend.
WITH orders AS (
    SELECT
        order_id,
        arbitrary(customer_id) AS customer_id,
        arbitrary(amount)      AS amount
    FROM %s.%s
    WHERE event_type = 'order_placed'
      AND CAST(event_date AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
    GROUP BY order_id
)
SELECT
    customer_id,
    round(SUM(amount), 2) AS total_spend,
    COUNT(*)              AS order_count
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > ?
ORDER BY total_spend DESC, customer_id
