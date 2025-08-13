

WITH sales_source AS (
  SELECT
    transaction_id,
    transaction_date,
    product_id,
    store_id,
    promotion_id,
    customer_id,
    quantity,
    unit_price,
    total_amount
  FROM "RetailSales"."oltp"."sales"
)
SELECT
*
from sales_source