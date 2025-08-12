
  
    

  create  table "RetailSales"."dw"."fct_promotion_eligibility__dbt_tmp"
  
  
    as
  
  (
    

-- generate one row per date the promotion is active
with promo_calendar as (

    select
        p.promotion_sk,
        d.date_sk
    from "RetailSales"."dw"."dim_promotions" p
    join "RetailSales"."dw"."dim_date" d
      on d.full_date between p.start_date and p.end_date

)

select
    date_sk,
    promotion_sk
from promo_calendar
order by promotion_sk, date_sk
  );
  