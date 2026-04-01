
        
            delete from "delta"."mart"."dim_date"
            where (
                date_key) in (
                select date_key
                from "delta"."mart"."dim_date__dbt_tmp"
            );

        
    

    insert into "delta"."mart"."dim_date" ("date_key", "full_date", "day_of_week", "day_name", "week_of_year", "month_num", "month_name", "quarter", "year", "is_weekend")
    (
        select "date_key", "full_date", "day_of_week", "day_name", "week_of_year", "month_num", "month_name", "quarter", "year", "is_weekend"
        from "delta"."mart"."dim_date__dbt_tmp"
    )