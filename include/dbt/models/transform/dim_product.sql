WITH dim_product_cte AS (
  SELECT 
    DISTINCT coffee_name, 
    money 
  FROM {{ source('sales', 'raw_sales') }}
)
SELECT 
  {{ dbt_utils.generate_surrogate_key(['coffee_name', 'money']) }} AS product_id, 
  coffee_name AS product_name, 
  money AS price 
from dim_product_cte