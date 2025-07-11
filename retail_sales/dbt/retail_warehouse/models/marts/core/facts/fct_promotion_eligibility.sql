{{ config(materialized='table') }}

-- Factless fact table: tracks product promotion eligibility per store and date
select 
    d.date_sk,
    p.product_sk,
    pr.promotion_sk,
    s.store_sk
from {{ ref('dim_promotions') }} pr
join {{ ref('dim_products') }} p     on 1=1
join {{ ref('dim_stores') }} s       on 1=1
join {{ ref('dim_date') }} d         on d.full_date between pr.start_date and pr.end_date