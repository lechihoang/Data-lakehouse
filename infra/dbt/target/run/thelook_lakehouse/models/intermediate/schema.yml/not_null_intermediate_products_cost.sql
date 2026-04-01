select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select cost
from "delta"."intermediate"."intermediate_products"
where cost is null



      
    ) dbt_internal_test