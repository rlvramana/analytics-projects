 --Event summary
    SELECT
  retailer_property_name,
  COUNT(DISTINCT CASE WHEN event_type = 'Product Detail' THEN event_id END) AS ProductDetail,
  COUNT(DISTINCT CASE WHEN event_type = 'Product List' THEN event_id END) AS ProductList,
  COUNT(DISTINCT CASE WHEN event_type = 'Add to Basket' THEN event_id END) AS add_to_basket,
  COUNT(DISTINCT CASE WHEN event_type = 'Basket View' THEN event_id END) AS  basket_view,
  COUNT(DISTINCT CASE WHEN event_type = 'Checkout' THEN event_id END) AS checkout,
  COUNT(DISTINCT CASE WHEN event_type = 'Purchase Confirmation' THEN event_id END) AS purchased
    
FROM
  staging_retailer_events

GROUP BY
  retailer_property_name
  ;

---feb to dec
SELECT
  retailer_property_name,
  COUNT(DISTINCT CASE WHEN event_type = 'Product Detail' THEN event_id END) AS ProductDetail,
  COUNT(DISTINCT CASE WHEN event_type = 'Product List' THEN event_id END) AS ProductList,
  COUNT(DISTINCT CASE WHEN event_type = 'Add to Basket' THEN event_id END) AS add_to_basket,
  COUNT(DISTINCT CASE WHEN event_type = 'Basket View' THEN event_id END) AS basket_view,
  COUNT(DISTINCT CASE WHEN lower(event_type) = 'checkout' THEN panelist_id END) AS checkout,
  COUNT(DISTINCT CASE WHEN event_type = 'Purchase Confirmation' THEN event_id END) AS purchased

FROM staging_retailer_events
WHERE 
  start_time_local::date >= (
    SELECT MAX(start_time_local::date) - INTERVAL '3 months'
    FROM staging_retailer_events_p
  )
GROUP BY retailer_property_name;

--product checkout
select event_type,product_name, count(Product_name) from staging_retailer_events sre   
where sre.event_type ='Checkout' and sre.retailer_property_name ='Amazon'
group by 1,2
------------

--panelist summary
SELECT
  retailer_property_name,
  COUNT(distinct CASE WHEN upper(event_type) = 'SEARCH' THEN panelist_id END) AS Search,
  COUNT(distinct CASE WHEN event_type = 'Product Detail' THEN panelist_id END) AS ProductDetail,
  COUNT(distinct CASE WHEN event_type = 'Product List' THEN panelist_id END) AS ProductList,
  COUNT(distinct CASE WHEN event_type = 'Add to Basket' THEN panelist_id END) AS added_to_basket,
  COUNT(distinct CASE WHEN event_type = 'Basket View' THEN panelist_id END) AS basket_view,
  COUNT(distinct CASE WHEN event_type = 'Checkout' THEN panelist_id END) AS checkout,
  COUNT(distinct CASE WHEN event_type = 'Purchase Confirmation' THEN panelist_id END) AS purchased

FROM
  staging_retailer_events_p
--where panelist_id='98358279-5236-4d35-b693-ab4f4eabffcf'
GROUP BY
  retailer_property_name;


select distinct event_type from staging_retailer_events sre limit 10


--purchase analysis
select product_name, event_type, srep.retailer_property_name , purchase_price, purchase_quantity from staging_retailer_events_p srep 
where event_type= 'Purchase Confirmation'
--where event_type= 'Add to Basket'
--and srep.purchase_price <>'''

--messy data
SELECT *
FROM staging_retailer_events_p
WHERE purchase_price LIKE '%[%' OR purchase_price LIKE '%]%'
LIMIT 100;


SELECT *
FROM staging_retailer_events
WHERE purchase_price LIKE '%[%' OR purchase_price LIKE '%]%'
LIMIT 100;

SELECT DISTINCT purchase_quantity
FROM staging_retailer_events
WHERE purchase_quantity LIKE '%[%' OR purchase_quantity LIKE '%]%';


select purchase_quantity from staging_retailer_events  where purchase_quantity like 'Max%'

select * from staging_retailer_events limit 100

SELECT
  UPPER(SUBSTRING(source_file, POSITION('=' IN source_file) + 1, 3)) AS month,
  '20' || SUBSTRING(source_file, POSITION('-' IN source_file) + 1, 2) AS year,
  COUNT(distinct source_file) AS file_count
FROM staging_retailer_events sre
GROUP BY
  UPPER(SUBSTRING(source_file, POSITION('=' IN source_file) + 1, 3)),
  SUBSTRING(source_file, POSITION('-' IN source_file) + 1, 2)
ORDER BY year, month;


select * from fashion_coding_cleaning fcc where fcc.relevant_code in (0,10) limit 10
select count(*) from fact_retailer_events
select count(*) from dim_panelist
select count(*) from fashion_coding_cleaning

--how many fashion items correctly categorized
select count(*) from staging_retailer_events sre inner join fashion_coding_cleaning fcc on sre.product_name =fcc.product_name
where fcc.relevant_code =1

select count(*) from staging_retailer_events sre inner join fashion_coding_cleaning fcc on sre.product_name =fcc.product_name
where fcc.relevant_code  in (0,10)

----how many missing rows in search and prodcut name
select count(*) from staging_retailer_events sre where sre.search_term =''
select count(*) from staging_retailer_events sre where sre.product_name=''

-------
select * from (
SELECT
  retailer_property_name,
  product_name,
  COUNT(DISTINCT CASE WHEN event_type = 'Product Detail' THEN event_id END) AS product_detail,
  COUNT(DISTINCT CASE WHEN event_type = 'Product List' THEN event_id END) AS product_list,
  COUNT(DISTINCT CASE WHEN event_type = 'Add to Basket' THEN event_id END) AS add_to_basket,
  COUNT(DISTINCT CASE WHEN event_type = 'Basket View' THEN event_id END) AS basket_view,
  COUNT(DISTINCT CASE WHEN event_type = 'Checkout' THEN event_id END) AS checkout,
  COUNT(DISTINCT CASE WHEN event_type = 'Purchase Confirmation' THEN event_id END) AS purchased,
  ROUND(
    COUNT(DISTINCT CASE WHEN event_type = 'Checkout' THEN event_id END) * 1.0 /
    NULLIF(COUNT(DISTINCT CASE WHEN event_type = 'Basket View' THEN event_id END), 0), 4
  )*100 AS conversion_rate
FROM
  staging_retailer_events
  where product_name<>''
GROUP BY
  retailer_property_name, product_name
  HAVING
  COUNT(DISTINCT CASE WHEN event_type = 'Checkout' THEN event_id END) > 0
  AND COUNT(DISTINCT CASE WHEN event_type = 'Basket View' THEN event_id END) > 0
) as table1 where conversion_rate>0 order by conversion_rate desc


-----duplciates

SELECT


---counts
SELECT
  sre.product_name,
  COUNT(DISTINCT CASE WHEN sre.event_type = 'Product Detail' THEN sre.event_id END) AS product_detail,
  COUNT(DISTINCT CASE WHEN sre.event_type = 'Checkout' THEN sre.event_id END) AS checkout,
  ROUND(
    COUNT(DISTINCT CASE WHEN sre.event_type = 'Checkout' THEN sre.event_id END) * 1.0 /
    NULLIF(COUNT(DISTINCT CASE WHEN sre.event_type = 'Product Detail' THEN sre.event_id END), 0), 4
  ) * 100 AS conversion_rate
FROM
  staging_retailer_events sre
INNER JOIN
  fashion_coding_cleaning fcc
  ON sre.product_name = fcc.product_name
WHERE
  sre.product_name <> ''
  AND fcc.relevant_code = 1
GROUP BY
  sre.product_name
HAVING
  (
    COUNT(DISTINCT CASE WHEN sre.event_type = 'Product Detail' THEN sre.event_id END) >
    COUNT(DISTINCT CASE WHEN sre.event_type = 'Checkout' THEN sre.event_id END)
  )
  OR
  (
    COUNT(DISTINCT CASE WHEN sre.event_type = 'Product Detail' THEN sre.event_id END) =
    COUNT(DISTINCT CASE WHEN sre.event_type = 'Checkout' THEN sre.event_id END)
    AND COUNT(DISTINCT CASE WHEN sre.event_type = 'Product Detail' THEN sre.event_id END) > 5
  )
ORDER BY
  conversion_rate desc
  
  
  
  
SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.purchase_price) AS median_price,
  avg(s.purchase_price )
FROM
  fact_retailer_events s
INNER JOIN
  fashion_coding_cleaning f
  ON s.product_name = f.product_name
WHERE
  s.event_type = 'Product Detail'
  AND s.purchase_price IS NOT NULL
  AND f.relevant_code = 1
  
    
  select count(distinct event_id) from staging_retailer_events  s inner join fashion_coding_cleaning f
    on s.product_name=f.product_name
where event_type='Product Detail'  and relevant_code=1 and f.product_name <>''
  

select * from staging_retailer_events cre limit 10000
select count(*) from cleaned_retailer_events where bad_purchase_quantity=true 



