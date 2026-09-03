-- Daily revenue and order counts by status (orders is the CDC-maintained current state)
SELECT
  to_date(created_at)            AS order_date,
  status,
  COUNT(*)                       AS orders,
  ROUND(SUM(total_amount), 2)    AS revenue,
  ROUND(AVG(total_amount), 2)    AS avg_order_value
FROM silver_orders
GROUP BY to_date(created_at), status
