
    
    



with __dbt__cte__staging_events as (


-- Web/app events — append-only, no dedup needed
SELECT * FROM "delta"."staging"."events"
) select event_type
from __dbt__cte__staging_events
where event_type is null


