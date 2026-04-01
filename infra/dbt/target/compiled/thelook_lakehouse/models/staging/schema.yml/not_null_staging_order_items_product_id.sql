
    
    



with __dbt__cte__staging_order_items as (


-- Latest state of each order item (deduplicate CDC via kafka_ts watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) AS rn
    FROM "delta"."staging"."order_items"
)
WHERE rn = 1
) select product_id
from __dbt__cte__staging_order_items
where product_id is null


