WITH fct_invoices_cte AS (
  SELECT
    datetime,
    to_hex(md5(cast(coalesce(cast(COALESCE(card, 'cash') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(cash_type as string), '_dbt_utils_surrogate_key_null_') as string))) AS customer_id,
    to_hex(md5(cast(coalesce(cast(coffee_name as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(money as string), '_dbt_utils_surrogate_key_null_') as string))) AS product_id,
    money
  FROM `atomic-graph-451814-f6`.`sales`.`raw_sales`
), aggregated_data AS (
  SELECT 
    CAST(DATE(datetime) AS STRING) AS invoice_date,
    customer_id,
    product_id,
    SUM(money) AS total
  FROM fct_invoices_cte
  GROUP BY invoice_date, customer_id, product_id
)
SELECT 
  to_hex(md5(cast(coalesce(cast(invoice_date as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(customer_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(product_id as string), '_dbt_utils_surrogate_key_null_') as string))) AS invoice_id,
  invoice_date,
  customer_id,
  product_id,
  total
FROM aggregated_data
ORDER BY invoice_date ASC, customer_id