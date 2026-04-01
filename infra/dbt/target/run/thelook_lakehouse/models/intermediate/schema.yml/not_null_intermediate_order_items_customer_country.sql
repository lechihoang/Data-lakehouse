select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_country
from "delta"."intermediate"."intermediate_order_items"
where customer_country is null



      
    ) dbt_internal_test