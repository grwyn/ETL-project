SELECT
    p.product_name,
    COUNT(1) AS total_sold,
    SUM(i.total) AS total_revenue
FROM `atomic-graph-451814-f6`.`sales`.`fct_invoices` i
JOIN `atomic-graph-451814-f6`.`sales`.`dim_product` p ON i.product_id = p.product_id
GROUP BY p.product_name
ORDER BY SUM(i.total) DESC