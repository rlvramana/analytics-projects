
  
    

  create  table "RetailSales"."dw"."dim_promotions__dbt_tmp"
  
  
    as
  
  (
    

-- -------------------------------------------------------------------------
-- 1.  Regular promotion rows from the staging table
-- -------------------------------------------------------------------------
with base as (

    select
        nextval('dw.dim_promotions_sk_seq') as promotion_sk,   -- surrogate
        promotion_id,
        promotion_name,
        media_type,
        discount_pct,
        start_date,
        end_date
    from "RetailSales"."oltp"."stg_promotions"

),

-- -------------------------------------------------------------------------
-- 2.  Kimball “Unknown Promotion” row  (surrogate key = 0)
-- -------------------------------------------------------------------------
unknown_row as (

    select
        0                    as promotion_sk,
        'UNK'                as promotion_id,
        'Unknown Promotion'  as promotion_name,
        'N/A'                as media_type,
        0.0                  as discount_pct,
        date '1900-01-01'    as start_date,
        date '2099-12-31'    as end_date

)

-- -------------------------------------------------------------------------
-- 3.  Final output
-- -------------------------------------------------------------------------
select * from base
union all
select * from unknown_row
  );
  