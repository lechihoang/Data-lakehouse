
        
            delete from "delta"."intermediate"."intermediate_events"
            where (
                event_id) in (
                select event_id
                from "delta"."intermediate"."intermediate_events__dbt_tmp"
            );

        
    

    insert into "delta"."intermediate"."intermediate_events" ("event_id", "session_id", "sequence_number", "event_type", "uri", "event_time", "city", "state", "postal_code", "browser", "traffic_source", "ip_address", "user_id", "customer_name", "customer_gender", "customer_age", "customer_country", "customer_lat", "customer_lon", "user_registered_at", "user_traffic_source", "is_ghost", "kafka_ts")
    (
        select "event_id", "session_id", "sequence_number", "event_type", "uri", "event_time", "city", "state", "postal_code", "browser", "traffic_source", "ip_address", "user_id", "customer_name", "customer_gender", "customer_age", "customer_country", "customer_lat", "customer_lon", "user_registered_at", "user_traffic_source", "is_ghost", "kafka_ts"
        from "delta"."intermediate"."intermediate_events__dbt_tmp"
    )