select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        price_tier as value_field,
        count(*) as n_records

    from "delta"."mart"."dim_products"
    group by price_tier

)

select *
from all_values
where value_field not in (
    'premium','mid','budget'
)



      
    ) dbt_internal_test