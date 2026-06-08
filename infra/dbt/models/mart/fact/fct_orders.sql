{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: 1 row per order
SELECT
    -- Keys
    o.order_id,
    o.user_id,
    CAST(date_format(date_trunc('day', o.kafka_ts), '%Y%m%d') AS INTEGER) AS date_key,
    -- Attributes
    o.order_status,
    o.num_of_items,
    o.traffic_source,
    -- Dates and timestamps (Avro stores as ISO string)
    TRY(CAST(o.created_at AS TIMESTAMP(6)))                             AS order_date,
    TRY(CAST(o.created_at AS TIMESTAMP(6)))                            AS order_created_at,
    TRY(CAST(o.shipped_at AS TIMESTAMP(6)))                            AS shipped_at,
    TRY(CAST(o.delivered_at AS TIMESTAMP(6)))                          AS delivered_at,
    TRY(CAST(o.returned_at AS TIMESTAMP(6)))                           AS returned_at,
    TRY(CAST(o.cancelled_at AS TIMESTAMP(6)))                         AS cancelled_at,
    -- Metadata
    o.kafka_ts

FROM {{ ref('intermediate_orders') }} o

{% if is_incremental() %}
WHERE o.kafka_ts > (SELECT MAX(kafka_ts) FROM {{ this }})
{% endif %}
