
        
            delete from "delta"."mart"."fct_events"
            where (
                event_id) in (
                select event_id
                from "delta"."mart"."fct_events__dbt_tmp"
            );

        
    

    insert into "delta"."mart"."fct_events" ("event_id", "user_id", "session_id", "date_key", "sequence_number", "event_type", "uri", "traffic_source", "browser", "ip_address", "city", "state", "postal_code", "is_ghost", "event_date", "event_time", "kafka_ts")
    (
        select "event_id", "user_id", "session_id", "date_key", "sequence_number", "event_type", "uri", "traffic_source", "browser", "ip_address", "city", "state", "postal_code", "is_ghost", "event_date", "event_time", "kafka_ts"
        from "delta"."mart"."fct_events__dbt_tmp"
    )