{{ config(materialized='table') }}

-- generate one row per date the promotion is active
with promo_calendar as (

    select
        p.promotion_sk,
        d.date_sk
    from {{ ref('dim_promotions') }} p
    join {{ ref('dim_date') }} d
      on d.full_date between p.start_date and p.end_date

)

select
    date_sk,
    promotion_sk
from promo_calendar
order by promotion_sk, date_sk