
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select promotion_sk as from_field
    from "RetailSales"."dw"."fct_sales"
    where promotion_sk is not null
),

parent as (
    select promotion_sk as to_field
    from "RetailSales"."dw"."dim_promotions"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test