
        
            delete from "delta"."intermediate"."intermediate_users"
            where (
                user_id) in (
                select user_id
                from "delta"."intermediate"."intermediate_users__dbt_tmp"
            );

        
    

    insert into "delta"."intermediate"."intermediate_users" ("user_id", "first_name", "last_name", "full_name", "email", "age", "gender", "street_address", "postal_code", "city", "state", "country", "latitude", "longitude", "traffic_source", "registered_at", "updated_at", "kafka_ts")
    (
        select "user_id", "first_name", "last_name", "full_name", "email", "age", "gender", "street_address", "postal_code", "city", "state", "country", "latitude", "longitude", "traffic_source", "registered_at", "updated_at", "kafka_ts"
        from "delta"."intermediate"."intermediate_users__dbt_tmp"
    )