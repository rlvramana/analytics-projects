
    
    

with child as (
    select customer_sk as from_field
    from "RetailSales"."dw"."fct_sales"
    where customer_sk is not null
),

parent as (
    select customer_sk as to_field
    from "RetailSales"."dw"."dim_customers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


