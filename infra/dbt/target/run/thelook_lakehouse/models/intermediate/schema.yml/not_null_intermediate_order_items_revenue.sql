select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select revenue
from "delta"."intermediate"."intermediate_order_items"
where revenue is null



      
    ) dbt_internal_test