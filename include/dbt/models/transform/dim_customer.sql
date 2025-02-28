SELECT DISTINCT 
  {{ dbt_utils.generate_surrogate_key([ "COALESCE(card, 'cash')", 'cash_type' ]) }} AS customer_id,
  COALESCE(card, 'cash') AS card_number
FROM {{ source('sales', 'raw_sales') }}