
        
            delete from "delta"."mart"."dim_customers"
            where (
                user_id) in (
                select user_id
                from "delta"."mart"."dim_customers__dbt_tmp"
            );

        
    

    insert into "delta"."mart"."dim_customers" ("user_id", "first_name", "last_name", "full_name", "email", "gender", "age", "age_group", "country", "state", "city", "latitude", "longitude", "traffic_source", "registered_at", "total_orders", "total_revenue", "first_order_at", "last_order_at", "is_repeat_customer", "customer_tier", "last_updated_ts")
    (
        select "user_id", "first_name", "last_name", "full_name", "email", "gender", "age", "age_group", "country", "state", "city", "latitude", "longitude", "traffic_source", "registered_at", "total_orders", "total_revenue", "first_order_at", "last_order_at", "is_repeat_customer", "customer_tier", "last_updated_ts"
        from "delta"."mart"."dim_customers__dbt_tmp"
    )