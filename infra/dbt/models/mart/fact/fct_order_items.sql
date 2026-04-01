{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: 1 row per order item — most granular fact, FK to all dims
SELECT
    -- Keys
    oi.order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    CAST(date_format(date_trunc('day', oi.kafka_ts), '%Y%m%d') AS INTEGER) AS date_key,
    -- Status
    oi.order_status,
    oi.item_status,
    -- Measures
    oi.quantity,
    oi.sale_price,
    oi.revenue,
    oi.gross_margin,
    oi.product_cost,
    -- Dates and timestamps (Avro stores as ISO string)
    TRY(CAST(oi.order_created_at AS DATE))                         AS order_date,
    TRY(CAST(oi.item_created_at AS DATE))                          AS item_date,
    TRY(CAST(oi.order_created_at AS TIMESTAMP))                    AS order_created_at,
    TRY(CAST(oi.item_created_at AS TIMESTAMP))                      AS item_created_at,
    TRY(CAST(oi.item_shipped_at AS TIMESTAMP))                    AS item_shipped_at,
    TRY(CAST(oi.item_delivered_at AS TIMESTAMP))                   AS item_delivered_at,
    TRY(CAST(oi.item_returned_at AS TIMESTAMP))                    AS item_returned_at,
    TRY(CAST(oi.item_cancelled_at AS TIMESTAMP))                   AS item_cancelled_at,
    -- Metadata
    oi.kafka_ts

FROM {{ ref('intermediate_order_items') }} oi

{% if is_incremental() %}
WHERE oi.kafka_ts > (SELECT MAX(kafka_ts) FROM {{ this }})
{% endif %}
