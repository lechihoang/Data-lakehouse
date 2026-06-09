

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
    TRY(CAST(oi.order_created_at AS TIMESTAMP(6)))                    AS order_created_at,
    TRY(CAST(oi.item_created_at AS TIMESTAMP(6)))                      AS item_created_at,
    TRY(CAST(oi.item_shipped_at AS TIMESTAMP(6)))                    AS item_shipped_at,
    TRY(CAST(oi.item_delivered_at AS TIMESTAMP(6)))                   AS item_delivered_at,
    TRY(CAST(oi.item_returned_at AS TIMESTAMP(6)))                    AS item_returned_at,
    TRY(CAST(oi.item_cancelled_at AS TIMESTAMP(6)))                   AS item_cancelled_at,
    -- Metadata
    oi.kafka_ts

FROM "delta"."intermediate"."intermediate_order_items" oi

