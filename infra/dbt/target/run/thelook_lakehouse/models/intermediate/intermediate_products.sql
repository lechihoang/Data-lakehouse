
        
            delete from "delta"."intermediate"."intermediate_products"
            where (
                product_id) in (
                select product_id
                from "delta"."intermediate"."intermediate_products__dbt_tmp"
            );

        
    

    insert into "delta"."intermediate"."intermediate_products" ("product_id", "product_name", "product_category", "product_department", "product_brand", "sku", "cost", "retail_price", "list_margin", "distribution_center_id", "distribution_center", "dc_latitude", "dc_longitude", "event_ts_ms")
    (
        select "product_id", "product_name", "product_category", "product_department", "product_brand", "sku", "cost", "retail_price", "list_margin", "distribution_center_id", "distribution_center", "dc_latitude", "dc_longitude", "event_ts_ms"
        from "delta"."intermediate"."intermediate_products__dbt_tmp"
    )