WITH dim_product_cte AS (
  SELECT 
    DISTINCT coffee_name, 
    money 
  FROM `atomic-graph-451814-f6`.`sales`.`raw_sales`
)
SELECT 
  to_hex(md5(cast(coalesce(cast(coffee_name as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(money as string), '_dbt_utils_surrogate_key_null_') as string))) AS product_id, 
  coffee_name AS product_name, 
  money AS price 
from dim_product_cte