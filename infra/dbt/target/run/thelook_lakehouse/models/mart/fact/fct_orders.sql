
        
            delete from "delta"."mart"."fct_orders"
            where (
                order_id) in (
                select order_id
                from "delta"."mart"."fct_orders__dbt_tmp"
            );

        
    

    insert into "delta"."mart"."fct_orders" ("order_id", "user_id", "date_key", "order_status", "num_of_items", "traffic_source", "order_date", "order_created_at", "shipped_at", "delivered_at", "returned_at", "cancelled_at", "kafka_ts")
    (
        select "order_id", "user_id", "date_key", "order_status", "num_of_items", "traffic_source", "order_date", "order_created_at", "shipped_at", "delivered_at", "returned_at", "cancelled_at", "kafka_ts"
        from "delta"."mart"."fct_orders__dbt_tmp"
    )