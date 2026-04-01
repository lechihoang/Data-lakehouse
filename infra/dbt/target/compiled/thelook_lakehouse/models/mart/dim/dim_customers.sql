

WITH



purchase_stats AS (
    SELECT
        user_id,
        COUNT(DISTINCT order_id)                   AS total_orders,
        ROUND(SUM(revenue), 2)                     AS total_revenue,
        MIN(oi.kafka_ts)                           AS first_order_at,
        MAX(oi.kafka_ts)                          AS last_order_at,
        MAX(oi.kafka_ts)                          AS last_order_ts
    FROM "delta"."intermediate"."intermediate_order_items" oi
    WHERE user_id IS NOT NULL
    
    GROUP BY 1
)

SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    u.full_name,
    u.email,
    u.gender,
    u.age,
    CASE
        WHEN u.age < 25 THEN '18-24'
        WHEN u.age < 35 THEN '25-34'
        WHEN u.age < 45 THEN '35-44'
        WHEN u.age < 55 THEN '45-54'
        ELSE '55+'
    END                                                               AS age_group,
    u.country,
    u.state,
    u.city,
    u.latitude,
    u.longitude,
    u.traffic_source,
    u.registered_at,
    COALESCE(p.total_orders, 0)                                      AS total_orders,
    COALESCE(p.total_revenue, CAST(0 AS DECIMAL(18, 2)))           AS total_revenue,
    p.first_order_at,
    p.last_order_at,
    CASE WHEN COALESCE(p.total_orders, 0) > 1 THEN TRUE ELSE FALSE END AS is_repeat_customer,
    CASE
        WHEN COALESCE(p.total_revenue, 0) >= 1000 THEN 'high'
        WHEN COALESCE(p.total_revenue, 0) >= 200  THEN 'medium'
        WHEN COALESCE(p.total_revenue, 0) >  0    THEN 'low'
        ELSE 'no_purchase'
    END                                                               AS customer_tier,
    GREATEST(u.kafka_ts, COALESCE(p.last_order_ts, TIMESTAMP '1970-01-01')) AS last_updated_ts

FROM "delta"."intermediate"."intermediate_users" u
LEFT JOIN purchase_stats p ON u.user_id = p.user_id

