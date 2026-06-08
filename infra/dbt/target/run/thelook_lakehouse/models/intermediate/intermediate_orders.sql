
        
            delete from "delta"."intermediate"."intermediate_orders"
            where (
                order_id) in (
                select order_id
                from "delta"."intermediate"."intermediate_orders__dbt_tmp"
            );

        
    

    insert into "delta"."intermediate"."intermediate_orders" ("order_id", "order_status", "num_of_items", "created_at", "updated_at", "shipped_at", "delivered_at", "returned_at", "cancelled_at", "user_id", "customer_name", "customer_gender", "customer_age", "customer_country", "customer_state", "customer_city", "customer_lat", "customer_lon", "user_registered_at", "traffic_source", "kafka_ts")
    (
        select "order_id", "order_status", "num_of_items", "created_at", "updated_at", "shipped_at", "delivered_at", "returned_at", "cancelled_at", "user_id", "customer_name", "customer_gender", "customer_age", "customer_country", "customer_state", "customer_city", "customer_lat", "customer_lon", "user_registered_at", "traffic_source", "kafka_ts"
        from "delta"."intermediate"."intermediate_orders__dbt_tmp"
    )