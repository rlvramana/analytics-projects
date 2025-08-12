
    
    

with child as (
    select store_sk as from_field
    from "RetailSales"."dw"."fct_promotion_eligibility"
    where store_sk is not null
),

parent as (
    select store_sk as to_field
    from "RetailSales"."dw"."dim_stores"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


