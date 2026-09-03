-- Product ranking inside each category using window functions
WITH sales AS (
  SELECT p.category, p.name AS product,
         SUM(oi.quantity)                              AS units,
         ROUND(SUM(oi.quantity * oi.unit_price), 2)    AS gross_revenue
  FROM silver_order_items oi
  JOIN silver_products p ON oi.product_id = p.id
  GROUP BY p.category, p.name
)
SELECT *,
       DENSE_RANK() OVER (PARTITION BY category ORDER BY gross_revenue DESC)              AS rank_in_category,
       ROUND(100 * gross_revenue / SUM(gross_revenue) OVER (PARTITION BY category), 2)   AS category_share_pct
FROM sales
