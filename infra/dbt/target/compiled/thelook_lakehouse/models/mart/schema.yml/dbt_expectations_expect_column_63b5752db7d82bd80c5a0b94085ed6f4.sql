






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and total_revenue >= 0 and total_revenue <= 1000000
)
 as expression


    from "delta"."mart"."dim_customers"
    

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







