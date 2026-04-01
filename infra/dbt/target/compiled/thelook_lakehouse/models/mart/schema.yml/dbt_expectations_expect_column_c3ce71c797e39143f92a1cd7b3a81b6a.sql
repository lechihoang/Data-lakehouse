






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and retail_price >= 0 and retail_price <= 10000
)
 as expression


    from "delta"."mart"."dim_products"
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







