
  
    

    create or replace table `atomic-graph-451814-f6`.`sales`.`report_customer_invoices`
      
    
    

    OPTIONS()
    as (
      SELECT
    i.customer_id,
    COUNT(1) AS order_times,
    SUM(i.total) AS total_spend
FROM `atomic-graph-451814-f6`.`sales`.`fct_invoices` i
JOIN `atomic-graph-451814-f6`.`sales`.`dim_customer` c ON i.customer_id = c.customer_id
GROUP BY i.customer_id
ORDER BY SUM(i.total) DESC
    );
  