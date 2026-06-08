
        
            delete from "delta"."intermediate"."intermediate_order_items"
            where (
                order_item_id) in (
                select order_item_id
                from "delta"."intermediate"."intermediate_order_items__dbt_tmp"
            );

        
    

    insert into "delta"."intermediate"."intermediate_order_items" ("order_item_id", "order_id", "order_status", "order_num_items", "order_created_at", "order_shipped_at", "order_delivered_at", "order_returned_at", "order_cancelled_at", "item_status", "quantity", "sale_price", "revenue", "item_created_at", "item_shipped_at", "item_delivered_at", "item_returned_at", "item_cancelled_at", "product_id", "product_name", "product_category", "product_brand", "product_department", "product_sku", "product_cost", "product_retail_price", "gross_margin", "distribution_center_id", "distribution_center", "user_id", "customer_name", "customer_gender", "customer_age", "customer_country", "customer_state", "customer_city", "customer_lat", "customer_lon", "user_registered_at", "traffic_source", "kafka_ts")
    (
        select "order_item_id", "order_id", "order_status", "order_num_items", "order_created_at", "order_shipped_at", "order_delivered_at", "order_returned_at", "order_cancelled_at", "item_status", "quantity", "sale_price", "revenue", "item_created_at", "item_shipped_at", "item_delivered_at", "item_returned_at", "item_cancelled_at", "product_id", "product_name", "product_category", "product_brand", "product_department", "product_sku", "product_cost", "product_retail_price", "gross_margin", "distribution_center_id", "distribution_center", "user_id", "customer_name", "customer_gender", "customer_age", "customer_country", "customer_state", "customer_city", "customer_lat", "customer_lon", "user_registered_at", "traffic_source", "kafka_ts"
        from "delta"."intermediate"."intermediate_order_items__dbt_tmp"
    )