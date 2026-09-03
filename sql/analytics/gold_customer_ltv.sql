-- Customer lifetime value against the *current* SCD2 version of each customer
SELECT
  c.id                                   AS customer_id,
  c.name, c.city, c.tier,
  COUNT(o.id)                            AS orders,
  ROUND(SUM(o.total_amount), 2)          AS lifetime_value,
  ROUND(AVG(o.total_amount), 2)          AS avg_order_value,
  NTILE(10) OVER (ORDER BY SUM(o.total_amount) DESC) AS value_decile
FROM silver_customers c
JOIN silver_orders o ON o.customer_id = c.id
WHERE c.is_current AND o.status <> 'cancelled'
GROUP BY c.id, c.name, c.city, c.tier
