

select 
     nextval('dw.dim_products_sk_seq') as product_sk,
     product_id,
     product_name,
     brand,
     subcategory,
     category,
     department,
     package_type,
     fat_content,
     diet_type
from "RetailSales"."oltp"."stg_products"