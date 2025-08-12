
  create view "RetailSales"."oltp"."stg_customers__dbt_tmp"
    
    
  as (
    

WITH customers_source AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        gender,
        birthdate,
        email,
        signup_date
    FROM "RetailSales"."oltp"."customers"
)

SELECT
    customer_id,
    first_name,
    last_name,
    gender,
    birthdate,
    email,
    signup_date
FROM customers_source
  );