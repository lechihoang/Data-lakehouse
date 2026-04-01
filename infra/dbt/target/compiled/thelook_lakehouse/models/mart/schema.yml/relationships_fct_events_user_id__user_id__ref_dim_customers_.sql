
    
    

with child as (
    select user_id as from_field
    from "delta"."mart"."fct_events"
    where user_id is not null
),

parent as (
    select user_id as to_field
    from "delta"."mart"."dim_customers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


