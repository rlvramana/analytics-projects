select * from fashion_labeled_cleaned limit 100
select count(distinct product_name) as unique_fashion from fashion_events_dep_final limit 100
select count(distinct product_name) as total_prodcuts from staging_retailer_events 
select count(distinct product_name) as qurious_data from fashion_coding_cleaning where relevant_code=1
relevant_code in (0,10)