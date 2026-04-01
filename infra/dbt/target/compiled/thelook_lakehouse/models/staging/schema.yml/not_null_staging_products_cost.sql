
    
    



with __dbt__cte__staging_products as (


-- Latest state of each product (deduplicate via event_ts_ms watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."staging"."products"
)
WHERE rn = 1
) select cost
from __dbt__cte__staging_products
where cost is null


