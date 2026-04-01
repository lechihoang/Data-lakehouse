

with __dbt__cte__staging_users as (


-- Latest state of each user (deduplicate CDC via kafka_ts watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) AS rn
    FROM "delta"."staging"."users"
)
WHERE rn = 1
) -- User dimension: latest profile state, enriched with purchase summary
SELECT
    u.id                            AS user_id,
    u.first_name,
    u.last_name,
    u.first_name || ' ' || u.last_name AS full_name,
    u.email,
    u.age,
    u.gender,
    u.street_address,
    u.postal_code,
    u.city,
    u.state,
    u.country,
    u.latitude,
    u.longitude,
    u.traffic_source,
    u.created_at                    AS registered_at,
    u.updated_at,
    u.kafka_ts

FROM __dbt__cte__staging_users u

