
  
    

    create table "delta"."intermediate"."intermediate_products"
      
      
    as (
      

with __dbt__cte__staging_products as (


-- Latest state of each product (deduplicate via event_ts_ms watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."staging"."ref_products"
)
WHERE rn = 1
),  __dbt__cte__staging_dist_centers as (


-- Latest state of each distribution center (deduplicate via event_ts_ms watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM "delta"."staging"."ref_dist_centers"
)
WHERE rn = 1
) -- Product dimension: latest catalog state with distribution center info
SELECT
    p.id                            AS product_id,
    p.name                          AS product_name,
    p.category                      AS product_category,
    p.department                    AS product_department,
    p.brand                         AS product_brand,
    p.sku,
    p.cost,
    p.retail_price,
    p.retail_price - p.cost         AS list_margin,
    p.distribution_center_id,
    dc.name                         AS distribution_center,
    dc.latitude                     AS dc_latitude,
    dc.longitude                    AS dc_longitude,
    p.event_ts_ms

FROM __dbt__cte__staging_products p
LEFT JOIN __dbt__cte__staging_dist_centers dc ON p.distribution_center_id = dc.id


    );

  