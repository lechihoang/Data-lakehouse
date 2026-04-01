
    
    



with __dbt__cte__staging_events as (


-- Web/app events — append-only, no dedup needed
SELECT * FROM "delta"."staging"."events"
) select session_id
from __dbt__cte__staging_events
where session_id is null


