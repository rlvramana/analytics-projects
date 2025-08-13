
select distinct event_type from staging_retailer_events sre 
select distinct panelist_id from staging_retailer_events where retailer_property_name='Amazon' 
limit 100 


SELECT
  retailer_property_name,
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


    SELECT
  retailer_property_name,
  COUNT(DISTINCT CASE WHEN event_type = 'Product Detail' THEN event_id END) AS ProductDetail,
  COUNT(DISTINCT CASE WHEN event_type = 'Product List' THEN event_id END) AS ProductList,
  COUNT(DISTINCT CASE WHEN event_type = 'Add to Basket' THEN event_id END) AS added_to_basket,
  COUNT(DISTINCT CASE WHEN event_type = 'Basket View' THEN event_id END) AS  basket_view,
  COUNT(DISTINCT CASE WHEN event_type = 'Checkout' THEN panelist_id END) AS checkout,
  COUNT(DISTINCT CASE WHEN event_type = 'Purchase Confirmation' THEN event_id END) AS purchased
    
FROM
  staging_retailer_events

GROUP BY
  retailer_property_name;
  
  

  

WITH viewers AS (
  SELECT DISTINCT panelist_id, retailer_property_name
  FROM staging_retailer_events_p
  WHERE event_type = 'Product Detail'
),
purchasers AS (
  SELECT DISTINCT panelist_id, retailer_property_name
  FROM staging_retailer_events_p
  WHERE event_type = 'Purchase Confirmation'
)

SELECT
  v.retailer_property_name,
  COUNT(DISTINCT v.panelist_id) AS total_viewers,
  COUNT(DISTINCT p.panelist_id) AS total_purchasers,
  ROUND(
    COUNT(DISTINCT p.panelist_id) * 100.0 / NULLIF(COUNT(DISTINCT v.panelist_id), 0),
    2
  ) AS conversion_rate_percent
FROM
  viewers v
LEFT JOIN
  purchasers p
  ON v.panelist_id = p.panelist_id
  AND v.retailer_property_name = p.retailer_property_name
GROUP BY
  v.retailer_property_name
ORDER BY
  v.retailer_property_name;


select count(distinct panelist_id) from staging_retailer_events_p srep 
select count(distinct panelist_id) from demos_p srep 
select count(distinct srep.panelist_id) from staging_retailer_events_p srep inner join demos_p p on srep.panelist_id =p.panelist_id
select count(distinct srep.panelist_id) from staging_retailer_events srep inner join panelist_demographics p on srep.panelist_id =p.panelist_id