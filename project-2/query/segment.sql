-- High-value customer segment over the curated order events (DuckDB).
-- The curated zone is at-least-once across ETL runs, so orders are de-duplicated first.
WITH orders AS (
    SELECT
        order_id,
        any_value(customer_id) AS customer_id,
        any_value(amount)      AS amount
    FROM read_parquet(getvariable('curated_glob'), hive_partitioning = true)
    WHERE event_type = 'order_placed'
      AND event_date >  CAST($as_of AS DATE) - CAST($window_days AS INTEGER)
      AND event_date <= CAST($as_of AS DATE)
    GROUP BY order_id
)
SELECT
    customer_id,
    round(SUM(amount), 2) AS total_spend,
    COUNT(*)              AS order_count
FROM orders
GROUP BY customer_id
HAVING SUM(amount) > $min_spend
ORDER BY total_spend DESC, customer_id;
