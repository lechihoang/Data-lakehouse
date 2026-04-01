select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select retail_price
from "delta"."intermediate"."intermediate_products"
where retail_price is null



      
    ) dbt_internal_test