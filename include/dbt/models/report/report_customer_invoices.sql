SELECT
    i.customer_id,
    COUNT(1) AS order_times,
    SUM(i.total) AS total_spend
FROM {{ ref('fct_invoices') }} i
JOIN {{ ref('dim_customer') }} c ON i.customer_id = c.customer_id
GROUP BY i.customer_id
ORDER BY SUM(i.total) DESC