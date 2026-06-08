
        
            delete from "delta"."mart"."fct_order_items"
            where (
                order_item_id) in (
                select order_item_id
                from "delta"."mart"."fct_order_items__dbt_tmp"
            );

        
    

    insert into "delta"."mart"."fct_order_items" ("order_item_id", "order_id", "user_id", "product_id", "date_key", "order_status", "item_status", "quantity", "sale_price", "revenue", "gross_margin", "product_cost", "order_date", "item_date", "order_created_at", "item_created_at", "item_shipped_at", "item_delivered_at", "item_returned_at", "item_cancelled_at", "kafka_ts")
    (
        select "order_item_id", "order_id", "user_id", "product_id", "date_key", "order_status", "item_status", "quantity", "sale_price", "revenue", "gross_margin", "product_cost", "order_date", "item_date", "order_created_at", "item_created_at", "item_shipped_at", "item_delivered_at", "item_returned_at", "item_cancelled_at", "kafka_ts"
        from "delta"."mart"."fct_order_items__dbt_tmp"
    )