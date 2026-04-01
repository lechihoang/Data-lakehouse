
  
    

    create table "delta"."intermediate"."intermediate_order_items"
      
      
    as (
      

with __dbt__cte__staging_order_items as (


-- Latest state of each order item (deduplicate CDC via kafka_ts watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) AS rn
    FROM "delta"."staging"."order_items"
)
WHERE rn = 1
),  __dbt__cte__staging_orders as (


-- Latest state of each order (deduplicate CDC via kafka_ts watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) AS rn
    FROM "delta"."staging"."orders"
)
WHERE rn = 1
),  __dbt__cte__staging_products as (


-- Latest state of each product (deduplicate via event_ts_ms watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."staging"."products"
)
WHERE rn = 1
),  __dbt__cte__staging_dist_centers as (


-- Latest state of each distribution center (deduplicate via event_ts_ms watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."staging"."dist_centers"
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
    oi.id                               AS order_item_id,
    oi.order_id,
    -- Order info
    o.status                            AS order_status,
    o.num_of_items                      AS order_num_items,
    o.created_at                        AS order_created_at,
    o.shipped_at                        AS order_shipped_at,
    o.delivered_at                      AS order_delivered_at,
    o.returned_at                       AS order_returned_at,
    o.cancelled_at                      AS order_cancelled_at,
    -- Item status
    oi.status                           AS item_status,
    oi.quantity,
    oi.sale_price,
    oi.quantity * oi.sale_price         AS revenue,
    oi.created_at                        AS item_created_at,
    oi.shipped_at                        AS item_shipped_at,
    oi.delivered_at                      AS item_delivered_at,
    oi.returned_at                       AS item_returned_at,
    oi.cancelled_at                      AS item_cancelled_at,
    -- Product
    oi.product_id,
    p.name                              AS product_name,
    p.category                          AS product_category,
    p.brand                             AS product_brand,
    p.department                        AS product_department,
    p.sku                               AS product_sku,
    p.cost                              AS product_cost,
    p.retail_price                      AS product_retail_price,
    oi.sale_price - p.cost              AS gross_margin,
    -- Distribution center
    p.distribution_center_id,
    dc.name                             AS distribution_center,
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
    oi.kafka_ts

FROM __dbt__cte__staging_order_items oi
LEFT JOIN __dbt__cte__staging_orders       o   ON oi.order_id               = o.id
LEFT JOIN __dbt__cte__staging_products     p   ON oi.product_id             = p.id
LEFT JOIN __dbt__cte__staging_dist_centers dc  ON p.distribution_center_id = dc.id
LEFT JOIN __dbt__cte__staging_users        u   ON o.user_id                 = u.id

WHERE oi.order_id IS NOT NULL

    );

  