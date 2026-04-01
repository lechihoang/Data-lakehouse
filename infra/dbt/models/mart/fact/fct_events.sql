{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: 1 row per event
SELECT
    -- Keys
    e.event_id,
    e.user_id,
    e.session_id,
    CAST(date_format(date_trunc('day', e.kafka_ts), '%Y%m%d') AS INTEGER) AS date_key,
    -- Attributes
    e.sequence_number,
    e.event_type,
    e.uri,
    e.traffic_source,
    e.browser,
    e.ip_address,
    -- Location
    e.city,
    e.state,
    e.postal_code,
    -- Flags
    e.is_ghost,
    -- Timestamps
    date(e.kafka_ts)        AS event_date,
    e.kafka_ts              AS event_time,
    e.kafka_ts

FROM {{ ref('intermediate_events') }} e

{% if is_incremental() %}
WHERE e.kafka_ts > (SELECT MAX(kafka_ts) FROM {{ this }})
{% endif %}
