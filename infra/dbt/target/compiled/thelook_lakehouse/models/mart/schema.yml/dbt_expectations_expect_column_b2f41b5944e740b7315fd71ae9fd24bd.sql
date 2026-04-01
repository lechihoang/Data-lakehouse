






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and total_events >= 1 and total_events <= 10000
)
 as expression


    from "delta"."mart"."fct_sessions"
    

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







