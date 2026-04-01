






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and revenue >= 0 and revenue <= 100000
)
 as expression


    from "delta"."intermediate"."intermediate_order_items"
    

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







