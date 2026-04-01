
  
    

    create table "delta"."intermediate"."intermediate_orders"
      
      
    as (
      

with __dbt__cte__staging_orders as (


-- Latest state of each order (deduplicate CDC via kafka_ts watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) AS rn
    FROM "delta"."staging"."orders"
)
WHERE rn = 1
),  __dbt__cte__staging_users as (


-- Latest state of each user (deduplicate CDC via kafka_ts watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) AS rn
    FROM "delta"."staging"."users"
)
WHERE rn = 1
) SELECT
    o.id                                AS order_id,
    o.status                            AS order_status,
    o.num_of_items,
    o.created_at,
    o.updated_at,
    o.shipped_at,
    o.delivered_at,
    o.returned_at,
    o.cancelled_at,
    -- User
    o.user_id,
    u.first_name || ' ' || u.last_name  AS customer_name,
    u.gender                            AS customer_gender,
    u.age                               AS customer_age,
    u.country                           AS customer_country,
    u.state                             AS customer_state,
    u.city                              AS customer_city,
    u.latitude                          AS customer_lat,
    u.longitude                         AS customer_lon,
    u.created_at                        AS user_registered_at,
    u.traffic_source,
    -- Metadata
    o.kafka_ts

FROM __dbt__cte__staging_orders o
LEFT JOIN __dbt__cte__staging_users u ON o.user_id = u.id

    );

  