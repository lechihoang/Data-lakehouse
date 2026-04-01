select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select distribution_center
from "delta"."intermediate"."intermediate_order_items"
where distribution_center is null



      
    ) dbt_internal_test