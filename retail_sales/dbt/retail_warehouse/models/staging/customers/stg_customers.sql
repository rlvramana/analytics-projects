{{ config(materialized='view') }}

WITH customers_source AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        gender,
        birthdate,
        email,
        signup_date
    FROM {{ source('oltp', 'customers') }}
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