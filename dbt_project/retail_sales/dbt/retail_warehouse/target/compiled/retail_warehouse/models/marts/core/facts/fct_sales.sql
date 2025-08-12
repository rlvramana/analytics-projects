

with src as (

    select * from "RetailSales"."oltp"."stg_sales"

), fact_rows as (

    select
        -- surrogate keys (coalesce to 0 for “Unknown”)
        coalesce(d.date_sk,       0) as date_sk,
        coalesce(c.customer_sk,   0) as customer_sk,
        coalesce(p.product_sk,    0) as product_sk,
        coalesce(s.store_sk,      0) as store_sk,
        coalesce(pr.promotion_sk, 0) as promotion_sk,

        -- numeric facts
        src.quantity,
        src.unit_price,
        src.quantity * src.unit_price as sales_amount

    from src
    left join "RetailSales"."dw"."dim_date"      d  on d.full_date     = src.transaction_date::date
    left join "RetailSales"."dw"."dim_customers" c  on c.customer_id   = src.customer_id
    left join "RetailSales"."dw"."dim_products"  p  on p.product_id    = src.product_id
    left join "RetailSales"."dw"."dim_stores"    s  on s.store_id      = src.store_id
    left join "RetailSales"."dw"."dim_promotions"pr on pr.promotion_id = src.promotion_id

)

select * from fact_rows