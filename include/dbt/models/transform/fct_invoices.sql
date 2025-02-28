WITH fct_invoices_cte AS (
  SELECT
    datetime,
    {{ dbt_utils.generate_surrogate_key([ "COALESCE(card, 'cash')", 'cash_type' ]) }} AS customer_id,
    {{ dbt_utils.generate_surrogate_key(['coffee_name', 'money']) }} AS product_id,
    money
  FROM {{ source('sales', 'raw_sales') }}
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
  {{ dbt_utils.generate_surrogate_key([ 'invoice_date', 'customer_id', 'product_id' ]) }} AS invoice_id,
  invoice_date,
  customer_id,
  product_id,
  total
FROM aggregated_data
ORDER BY invoice_date ASC, customer_id