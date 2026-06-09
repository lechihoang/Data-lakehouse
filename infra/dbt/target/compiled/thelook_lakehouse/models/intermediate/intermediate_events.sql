

WITH  __dbt__cte__staging_events as (


-- Web/app events — append-only, no dedup needed
SELECT * FROM "delta"."staging"."events"
),  __dbt__cte__staging_users as (


-- Latest state of each user (deduplicate CDC via kafka_ts watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) AS rn
    FROM "delta"."staging"."users"
)
WHERE rn = 1
), raw_events AS (
    SELECT * FROM __dbt__cte__staging_events
),

deduped_events AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) as rn
        FROM raw_events
    )
    WHERE rn = 1
)

SELECT
    e.id                                AS event_id,
    e.session_id,
    e.sequence_number,
    e.event_type,
    e.uri,
    e.created_at                        AS event_time,
    -- Location
    e.city,
    e.state,
    e.postal_code,
    -- Tech
    e.browser,
    e.traffic_source,
    e.ip_address,
    -- User (nullable — ghost events have no user)
    e.user_id,
    u.first_name || ' ' || u.last_name  AS customer_name,
    u.gender                            AS customer_gender,
    u.age                               AS customer_age,
    u.country                           AS customer_country,
    u.latitude                          AS customer_lat,
    u.longitude                         AS customer_lon,
    u.created_at                        AS user_registered_at,
    u.traffic_source                    AS user_traffic_source,
    CASE WHEN e.user_id IS NULL THEN TRUE ELSE FALSE END AS is_ghost,
    -- Metadata
    e.kafka_ts

FROM deduped_events e
LEFT JOIN __dbt__cte__staging_users u ON e.user_id = u.id
