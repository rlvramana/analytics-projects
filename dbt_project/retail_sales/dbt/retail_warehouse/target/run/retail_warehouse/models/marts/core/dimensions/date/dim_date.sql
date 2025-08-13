
  
    

  create  table "RetailSales"."dw"."dim_date__dbt_tmp"
  
  
    as
  
  (
    

with calendar as (
    select
        generate_series('2015-01-01'::date, '2035-12-31'::date, interval '1 day')::date as date_day
), base as (
    select
        (extract(year from date_day)*10000
         + extract(month from date_day)*100
         + extract(day   from date_day))::int      as date_sk,
        date_day                                    as full_date,
        extract(year from date_day)::int            as year,
        extract(quarter from date_day)::int         as quarter,
        extract(month from date_day)::int           as month,
        to_char(date_day,'Month')                   as month_name,
        extract(day from date_day)::int             as day_of_month,
        extract(doy from date_day)::int             as day_of_year,
        extract(week from date_day)::int            as week_of_year,
        extract(isodow from date_day)::int          as weekday_iso,
        to_char(date_day,'Day')                     as weekday_name,
        case when extract(isodow from date_day) in (6,7) then true else false end as is_weekend
    from calendar
), unknown as (
    /* the Kimball unknown row */
    select
        0                    as date_sk,
        '1900-01-01'::date   as full_date,
        1900                 as year,
        0                    as quarter,
        0                    as month,
        'Unknown'            as month_name,
        0  as day_of_month,
        0  as day_of_year,
        0  as week_of_year,
        0  as weekday_iso,
        'Unknown'            as weekday_name,
        false as is_weekend
)

select * from base
union all
select * from unknown
  );
  