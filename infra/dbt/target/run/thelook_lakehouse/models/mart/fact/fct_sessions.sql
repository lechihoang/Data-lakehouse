
        
            delete from "delta"."mart"."fct_sessions"
            where (
                session_id) in (
                select session_id
                from "delta"."mart"."fct_sessions__dbt_tmp"
            );

        
    

    insert into "delta"."mart"."fct_sessions" ("session_id", "user_id", "date_key", "traffic_source", "browser", "city", "state", "customer_country", "is_ghost", "hit_home", "hit_browse", "hit_product", "hit_cart", "hit_purchase", "hit_cancel", "hit_return", "session_date", "total_events", "session_start_at", "session_end_at", "session_duration_seconds", "last_event_ts")
    (
        select "session_id", "user_id", "date_key", "traffic_source", "browser", "city", "state", "customer_country", "is_ghost", "hit_home", "hit_browse", "hit_product", "hit_cart", "hit_purchase", "hit_cancel", "hit_return", "session_date", "total_events", "session_start_at", "session_end_at", "session_duration_seconds", "last_event_ts"
        from "delta"."mart"."fct_sessions__dbt_tmp"
    )