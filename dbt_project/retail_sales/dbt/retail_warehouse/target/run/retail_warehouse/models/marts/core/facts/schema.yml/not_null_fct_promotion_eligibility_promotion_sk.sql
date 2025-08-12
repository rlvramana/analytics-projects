
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select promotion_sk
from "RetailSales"."dw"."fct_promotion_eligibility"
where promotion_sk is null



  
  
      
    ) dbt_internal_test