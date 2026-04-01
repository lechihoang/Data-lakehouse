
    
    

with all_values as (

    select
        customer_tier as value_field,
        count(*) as n_records

    from "delta"."mart"."dim_customers"
    group by customer_tier

)

select *
from all_values
where value_field not in (
    'high','medium','low','no_purchase'
)


