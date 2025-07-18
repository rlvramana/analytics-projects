{{ config(materialized='view') }}

WITH products_source AS (
  SELECT
      product_id,
      product_name,
      brand,
      subcategory,
      category,
      department,
      package_type,
      fat_content,
      diet_type,
      current_price
  FROM {{ source('oltp', 'products') }}
)

SELECT
    product_id,
    product_name,
    brand,
    subcategory,
    category,
    department,
    package_type,
    fat_content,
    diet_type,
    current_price
FROM products_source