{{ config(materialized='view') }}

WITH promotions_source AS (
    SELECT
        promotion_id,
        promotion_name,
        media_type,
        start_date,
        end_date,
        discount_pct
    FROM {{ source('oltp', 'promotions') }}
)

SELECT
    promotion_id,
    promotion_name,
    media_type,
    start_date,
    end_date,
    discount_pct
FROM promotions_source