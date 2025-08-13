select count(distinct product_name_raw) from cleaned_retailer_events   fcc where fcc.relevant_code is not null

select count(*) from fashion_labeled_cleaned
select *from fashion_labeled_cleaned

select product_name from staging_retailer_events sre  where lower(product_name) like '%sex%' and 
lower(product_name) like '%ring%' and lower(product_name) like'%Men%'

select count(product_name_raw) from fashion_labeled_cleaned flc where split_type='val'
select * from 
--fashion_labeled_cleaned
cleaned_retailer_events where relevant_code is not null

select count(distinct product_name) from staging_retailer_events sre 
select count(distinct search_term) from staging_retailer_events sre 
select count(distinct product_name_raw) from fashion_labeled_cleaned sre
select count(distinct product_name) from fashion_events_final 


select count(*) from  fashion_events_final limit 10
select count(*) from  fashion_events_dep
select count(*) from  fashion_events_dep_final
select * from dim_panelist  limit 10


select count(distinct flc.product_name_raw)  from fashion_labeled_cleaned flc where split_type is  null and flc.relevant_code_binary =0



WITH positives AS (
    SELECT *
    FROM fashion_labeled_cleaned
    WHERE relevant_code_binary = 1 AND split_type IS NULL
),
negatives AS (
    SELECT *
    FROM fashion_labeled_cleaned
    WHERE relevant_code_binary = 0 AND split_type IS NULL
    ORDER BY random()
    LIMIT 1748
)
SELECT product_name_raw,relevant_code_binary FROM positives
UNION ALL
SELECT product_name_raw,relevant_code_binary FROM negatives