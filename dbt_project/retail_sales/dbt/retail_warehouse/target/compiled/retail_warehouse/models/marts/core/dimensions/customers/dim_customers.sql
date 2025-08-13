

select 
     nextval('dw.dim_customers_sk_seq') as customer_sk
    ,customer_id
    ,first_name
    ,last_name
    ,gender
    ,birthdate
    ,email
    ,signup_date
from "RetailSales"."oltp"."stg_customers"