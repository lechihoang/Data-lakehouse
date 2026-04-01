
    
    

with  __dbt__cte__staging_orders as (


-- Latest state of each order (deduplicate CDC via kafka_ts watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) AS rn
    FROM "delta"."staging"."orders"
)
WHERE rn = 1
), all_values as (

    select
        status as value_field,
        count(*) as n_records

    from __dbt__cte__staging_orders
    group by status

)

select *
from all_values
where value_field not in (
    'Processing','Shipped','Delivered','Cancelled','Returned'
)


