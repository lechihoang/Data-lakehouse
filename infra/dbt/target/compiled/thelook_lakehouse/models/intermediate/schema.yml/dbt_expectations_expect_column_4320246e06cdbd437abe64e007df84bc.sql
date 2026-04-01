






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and cost >= 0 and cost <= 10000
)
 as expression


    from "delta"."intermediate"."intermediate_products"
    

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







