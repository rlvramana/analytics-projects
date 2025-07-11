{{ config(materialized='table') }}

select 
     nextval('dw.dim_stores_sk_seq') as store_sk,
     store_id,
     store_name,
     district,
     region,
     open_date,
     remodel_date,
     store_type
from {{ ref('stg_stores') }}