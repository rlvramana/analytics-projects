
  create view "RetailSales"."oltp"."stg_stores__dbt_tmp"
    
    
  as (
    

WITH stores_source AS (
  SELECT
    store_id,
    store_name,
    district,
    region,
    open_date,
    remodel_date,
    store_type
  FROM "RetailSales"."oltp"."stores"
)

SELECT
  store_id,
  store_name,
  district,
  region,
  open_date,
  remodel_date,
  store_type
FROM stores_source
  );