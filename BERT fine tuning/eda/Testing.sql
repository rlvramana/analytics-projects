
select count(*) from fashion_events_final
select count(*) from dim_panelist
select count(*) from demos_p

select count(*) from fashion_events_final where purchase_price is not null
select count(*) from fashion_events_final where event_type='Checkout' 
SELECT sum(purchase_price)
--count(*)
  --  f.*,
   -- d.*
FROM fashion_events_final f
JOIN dim_panelist d
--join demos_p d
    ON f.panelist_id = d.panelist_id;
select sum(purchase_price) from fashion_events_final fef 
select * from fashion_events_final limit 100
select * from fashion_coding_cleaning fcc where relevant_code=1
select count(distinct product_name_raw) from cleaned_retailer_events cre where relevant_code_binary is not null
select count(distinct product_name_raw) from fashion_labeled_cleaned cre where relevant_code_binary is not null
select * from cleaned_retailer_events cre where cre.relevant_code_binary is not null


SELECT count(distinct cre.product_name_raw )
FROM cleaned_retailer_events cre
JOIN fashion_labeled_cleaned flc
  ON cre.product_name_raw = flc.product_name_raw
WHERE cre.relevant_code_binary IS NULL
  AND flc.relevant_code_binary IS NOT NULL;

    SELECT DISTINCT product_name_raw
    FROM cleaned_retailer_events
     WHERE relevant_code_binary IS NULL
     LIMIT 10000
     
     
     
     select product_name from staging_retailer_events where lower(product_name) like ('%sleep token%')
     
     select count(distinct cleaned_product_name) from filtered_retailer_events limit 100
     select count(*) from filtered_retailer_events
     select distinct department from filtered_retailer_events 
     select count(distinct product_name) from fashion_events_dep_final 
     
     
     select count(distinct product_name) from staging_retailer_events sre 
      select count(distinct original_product_name) from filtered_retailer_events sre 
       select count(distinct product_name) from fashion_events_dep_final 
       
       
       select count(distinct product_name) from fact_retailer_events fre inner join filtered_retailer_events f 
       on fre.product_name =f.original_product_name
       
select *
from fact_retailer_events fre
where exists (
  select 1
  from filtered_retailer_events f
  where f.original_product_name = fre.product_name
);

    select  original_product_name from filtered_retailer_events sre limit 100

       select product_name from fashion_events_dep_final fedf where lower(product_name) like '%wig%'
       select count(distinct product_name) from fact_retailer_events
       select count(distinct product_name) from fashion_coding_cleaning where relevant_code=1
       select distinct relevant_code from fashion_coding_cleaning limit 10
       
       create index if not exists idx_fact_product_name
on fact_retailer_events(product_name);
       
select count(distinct fre.product_name)
from fact_retailer_events fre
where exists (
    select 1
    from filtered_retailer_events f
    where f.original_product_name = fre.product_name
)
and exists (
    select 1
    from dim_panelist dp
    where dp.id = fre.panelist_key
);


select count(distinct fre.original_product_name ) from filtered_retailer_events fre 




SELECT
    e.event_type,
      COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'product detail') AS product_detail_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'add to basket') AS add_to_basket_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'checkout') AS checkout_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'product list') AS product_list_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'search') AS search_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'purchase confirmation') AS purchase_confirmation_count
FROM fact_retailer_events e
JOIN dim_panelist p
    ON e.panelist_id = p.panelist_id
JOIN fashion_coding_cleaning c
    ON e.product_name = c.product_name
GROUP BY e.event_type
ORDER BY e.event_type;





SELECT
    e.event_type,
  COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'product detail') AS product_detail_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'add to basket') AS add_to_basket_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'checkout') AS checkout_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'product list') AS product_list_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'search') AS search_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'purchase confirmation') AS purchase_confirmation_count
FROM fact_retailer_events e
JOIN dim_panelist p
    ON e.panelist_id = p.panelist_id
INNER JOIN filtered_retailer_events f
    ON e.product_name = f.original_product_name
GROUP BY e.event_type
ORDER BY e.event_type;


SELECT
    e.retailer_property_name AS retailer,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'product detail') AS product_detail_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'add to basket') AS add_to_basket_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'checkout') AS checkout_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'product list') AS product_list_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'search') AS search_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'purchase confirmation') AS purchase_confirmation_count
FROM fact_retailer_events e
JOIN dim_panelist p
    ON e.panelist_id = p.panelist_id
INNER JOIN filtered_retailer_events f
    ON e.product_name = f.original_product_name
GROUP BY e.retailer_property_name
ORDER BY retailer;

select count(distinct product_name), count(distinct event_id) from staging_retailer_events sre 
select count(distinct product_name), count(distinct event_id) from fashion_events_dep_final sre 
select count(distinct panelist_id) from panelist_demographics
select count(distinct panelist_id) from fashion_events_dep_final




SELECT
    e.retailer_property_name AS retailer,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'product detail') AS product_detail_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'add to basket') AS add_to_basket_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'checkout') AS checkout_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'product list') AS product_list_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'search') AS search_count,
    COUNT(DISTINCT e.event_id) FILTER (WHERE LOWER(e.event_type) = 'purchase confirmation') AS purchase_confirmation_count
FROM staging_retailer_events e
JOIN dim_panelist p
    ON e.panelist_id = p.panelist_id
GROUP BY e.retailer_property_name
ORDER BY retailer;

select (product_name) from staging_retailer_events sre where event_type='Purchase Confirmation' and sre.retailer_property_name ='Amazon'
select count(distinct original_product_name) from filtered_retailer_events

WITH counts AS (
  SELECT
    SUM(CASE WHEN relevant_code = 1 THEN 1 ELSE 0 END) AS count_1,
    SUM(CASE WHEN relevant_code IN (0, 10) THEN 1 ELSE 0 END) AS count_0_10,
    COUNT(*) AS total_count
  FROM fashion_coding_cleaning
)

SELECT
  count_1,
  count_0_10,
  ROUND(count_1 * 100.0 / total_count, 2) AS percent_1,
  ROUND(count_0_10 * 100.0 / total_count, 2) AS percent_0_10
FROM counts;

select distinct event_type from staging_retailer_events sre 


select count(distinct product_name) from staging_retailer_events
select count(distinct product_name) from fashion_events_dep_final
select count(distinct original_product_name) from filtered_retailer_events
select count(distinct product_name) from fashion_coding_cleaning where relevant_code in (0,10)

SELECT count(DISTINCT s.product_name)
FROM staging_retailer_events s
LEFT JOIN filtered_retailer_events f
    ON s.product_name = f.original_product_name
WHERE f.original_product_name IS NULL;

select original_product_name from filtered_retailer_events fre limit 1000
select product_name from fashion_events_dep_final fre limit 1000

select count(distinct product_name_raw) from fashion
select count(distinct product_name_raw) from non_fashion



SELECT *
FROM fashion
WHERE 
    product_name_raw ILIKE '%Toothbrush%'
    OR product_name_raw ILIKE '%Colgate%'
    OR product_name_raw ILIKE '%Samsung%'
    OR product_name_raw ILIKE '%iPad%'
    OR product_name_raw ILIKE '%Incontinence%'
    OR product_name_raw ILIKE '%Knee braces%'
    OR product_name_raw ILIKE '%Compression sleeve%'
    OR product_name_raw ILIKE '%Arthritis%'
    OR product_name_raw ILIKE '%Knee pain%'
    OR product_name_raw ILIKE '%knee brace%'
    OR product_name_raw ILIKE '%wigs%'
    OR product_name_raw ILIKE '%wig%'
    OR product_name_raw ILIKE '%human hair%'
    OR product_name_raw ILIKE '%memory foam%'
    OR product_name_raw ILIKE '%Rechargeable%'
    OR product_name_raw ILIKE '%deodorant%'
    OR product_name_raw ILIKE '%antiperspirant%'
    OR product_name_raw ILIKE '%postpartum%'
    OR product_name_raw ILIKE '%adult incontinence%'
    OR product_name_raw ILIKE '%Moisture control%'
    OR product_name_raw ILIKE '%shampoo%'
    OR product_name_raw ILIKE '%conditioner%'
    OR product_name_raw ILIKE '%vitamin%'
    OR product_name_raw ILIKE '%vitamins%'
    OR product_name_raw ILIKE '%Halloween%'
    OR product_name_raw ILIKE '%plantar fasciitis pain%'
    OR product_name_raw ILIKE '%plantar%'
    OR product_name_raw ILIKE '%SmartWatch%'
    OR product_name_raw ILIKE '%Braiding hair%'
    OR product_name_raw ILIKE '%extensions%'
    OR product_name_raw ILIKE '%Blanket%'
    OR product_name_raw ILIKE '%Hair box%'
    OR product_name_raw ILIKE '%crotchet braids%'
    OR product_name_raw ILIKE '%hair accessories%'
    OR product_name_raw ILIKE '%No Talk%'
    OR product_name_raw ILIKE '%Makeup Set%'
    OR product_name_raw ILIKE '%Dinnerware%'
    OR product_name_raw ILIKE '%Dishwasher%'
    OR product_name_raw ILIKE '%Microwave Safe%'
    OR product_name_raw ILIKE '%Tableware%'
    OR product_name_raw ILIKE '%Supplements%'
    OR product_name_raw ILIKE '%Probiotics%'
    OR product_name_raw ILIKE '%Stain remover%'
    OR product_name_raw ILIKE '%ankle brace%'
    OR product_name_raw ILIKE '%Dinosaur%'
    OR product_name_raw ILIKE '%smartwatch%'
    OR product_name_raw ILIKE '%twist hair%'
    OR product_name_raw ILIKE '%sciatica%'
    OR product_name_raw ILIKE '%hamstring%'
    OR product_name_raw ILIKE '%Baby stroller%'
    OR product_name_raw ILIKE '%Candy%'
    OR product_name_raw ILIKE '%Moisturizer%'
    OR product_name_raw ILIKE '%Blisters%'
    OR product_name_raw ILIKE '%Toe sleeves%'
    OR product_name_raw ILIKE '%Cup%'
    OR product_name_raw ILIKE '%Cups%';


select count(*) from  fashion_labeled_cleaned flc 
select product_name, relevant_code from fashion_coding_cleaning where relevant_code=1 limit 100



select distinct split_type from fashion_labeled_cleaned flc 

SELECT count(*)
FROM staging_retailer_events sre 
WHERE purchase_price LIKE '%[%"%'
   OR purchase_quantity LIKE '%[%"%';



select * from fashion_labeled_cleaned flc 


select count(distinct panelist_id) from dim_panelist dp 

select count(distinct product_name) from fashion_events_dep_final fedf 

select count(distinct search_term) from fashion_events_dep_final fedf 
select distinct search_term from fashion_events_dep_final fedf 