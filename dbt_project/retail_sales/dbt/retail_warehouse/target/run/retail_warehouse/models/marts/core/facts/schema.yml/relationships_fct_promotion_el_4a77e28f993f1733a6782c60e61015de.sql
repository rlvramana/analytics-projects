
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select date_sk as from_field
    from "RetailSales"."dw"."fct_promotion_eligibility"
    where date_sk is not null
),

parent as (
    select date_sk as to_field
    from "RetailSales"."dw"."dim_date"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test