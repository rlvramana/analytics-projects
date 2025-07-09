{{ config(materialized='view') }}

WITH stores_source AS (
  SELECT
    store_id,
    store_name,
    district,
    region,
    open_date,
    remodel_date,
    store_type
  FROM {{ source('oltp', 'stores') }}
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