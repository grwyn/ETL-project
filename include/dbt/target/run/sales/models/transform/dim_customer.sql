
  
    

    create or replace table `atomic-graph-451814-f6`.`sales`.`dim_customer`
      
    
    

    OPTIONS()
    as (
      SELECT DISTINCT 
  to_hex(md5(cast(coalesce(cast(COALESCE(card, 'cash') as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(cash_type as string), '_dbt_utils_surrogate_key_null_') as string))) AS customer_id,
  COALESCE(card, 'cash') AS card_number
FROM `atomic-graph-451814-f6`.`sales`.`raw_sales`
    );
  