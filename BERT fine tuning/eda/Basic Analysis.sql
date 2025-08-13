select * from staging_retailer_events sre limit 100
select distinct retailer_property_name from staging_retailer_events 


SELECT COUNT(*) AS hh_cleaning_count
FROM staging_normalized
WHERE normalized_product ILIKE '%paper towel%'
   OR normalized_product ILIKE '%toilet paper%'
   OR normalized_product ILIKE '%laundry detergent%'
   OR normalized_product ILIKE '%dish soap%'
   OR normalized_product ILIKE '%cleaning product%'
   OR normalized_product ILIKE '%cleaning equipment%'
   OR normalized_product ILIKE '%aluminum foil%'
   OR normalized_product ILIKE '%trash bag%'
   OR normalized_product ILIKE '%plastic bag%'
   OR normalized_product ILIKE '%disinfectant%'
;

SELECT COUNT(*) AS electronics_count
FROM staging_normalized
WHERE normalized_product ILIKE '%computer%'
   OR normalized_product ILIKE '%laptop%'
   OR normalized_product ILIKE '%tablet%'
   OR normalized_product ILIKE '%phone%'
   OR normalized_product ILIKE '%tv%'
   OR normalized_product ILIKE '%gaming console%'
   OR normalized_product ILIKE '%memory card%'
   OR normalized_product ILIKE '%camera%'
   OR normalized_product ILIKE '%monitor%'
   OR normalized_product ILIKE '%hard drive%'
;

SELECT COUNT(*) AS fashion_count
FROM staging_normalized
WHERE normalized_product ILIKE '%clothing%'
   OR normalized_product ILIKE '%shoe%'
   OR normalized_product ILIKE '%belt%'
   OR normalized_product ILIKE '%purse%'
   OR normalized_product ILIKE '%handbag%'
   OR normalized_product ILIKE '%baggy%'
   OR normalized_product ILIKE '%jewelry%'
   OR normalized_product ILIKE '%dress%'
   OR normalized_product ILIKE '%boots%'
   OR normalized_product ILIKE '%jeans%'
   -- etc.
;

SELECT COUNT(*) AS food_drink_count
FROM staging_normalized
WHERE normalized_product ILIKE '%soda%'
   OR normalized_product ILIKE '%water%'
   OR normalized_product ILIKE '%juice%'
   OR normalized_product ILIKE '%fresh produce%'
   OR normalized_product ILIKE '%snack%'
   OR normalized_product ILIKE '%cereal%'
   OR normalized_product ILIKE '%milk%'
   OR normalized_product ILIKE '%bread%'
   OR normalized_product ILIKE '%frozen%'
   OR normalized_product ILIKE '%chicken%'
   -- etc.
;

SELECT COUNT(*) AS beauty_count
FROM staging_normalized
WHERE normalized_product ILIKE '%face wash%'
   OR normalized_product ILIKE '%skin care%'
   OR normalized_product ILIKE '%hair care%'
   OR normalized_product ILIKE '%makeup%'
   OR normalized_product ILIKE '%foundation%'
   OR normalized_product ILIKE '%lipstick%'
   OR normalized_product ILIKE '%shampoo%'
   OR normalized_product ILIKE '%conditioner%'
   -- etc.
;


SELECT normalized_product AS fashion_count
FROM staging_normalized
WHERE normalized_product ILIKE '%clothing%'
   OR normalized_product ILIKE '%shoe%'
   OR normalized_product ILIKE '%belt%'
   OR normalized_product ILIKE '%purse%'
   OR normalized_product ILIKE '%handbag%'
   OR normalized_product ILIKE '%baggy%'
   OR normalized_product ILIKE '%jewelry%'
   OR normalized_product ILIKE '%dress%'
   OR normalized_product ILIKE '%boots%'
   OR normalized_product ILIKE '%jeans%'
limit 100;

SELECT normalized_product AS fashion_count
FROM staging_normalized
WHERE normalized_product ILIKE '%clothing%'
   OR normalized_product ILIKE '%shoe%'
   OR normalized_product ILIKE '%belt%'
   OR normalized_product ILIKE '%purse%'
   OR normalized_product ILIKE '%bag%'

   OR normalized_product ILIKE '%jewelry%'
   OR normalized_product ILIKE '%dress%'
   OR normalized_product ILIKE '%boots%'
   OR normalized_product ILIKE '%jeans%'
limit 100;


select normalized_product FROM staging_normalized 
where normalized_product ILIKE '%handbag%' 
or normalized_product ILIKE '%baggy%' 
limit 50

select distinct retailer_property_name from staging_normalized sn 


WITH words AS (
  SELECT
    unnest(
      regexp_split_to_array(
        lower(normalized_product),
        E'\\s+'
      )
    ) AS token
  FROM staging_normalized
)
SELECT token, COUNT(*) AS frequency
FROM words
GROUP BY token
ORDER BY frequency DESC
LIMIT 50;  -- Show top 50 words




CREATE TABLE IF NOT EXISTS fashion_cleaning_lookup (
    id SERIAL PRIMARY KEY,
    search_term TEXT,
    product_name TEXT,
    count INT,
    relevant_code INT
);


select count(*) from fashion_coding_cleaning

SELECT s.product_name AS staging_name,
       f.product_name AS cleaning_name,
       similarity(lower(s.product_name), lower(f.product_name)) AS sim_score
FROM staging_retailer_events s
JOIN fashion_coding_cleaning f
  ON lower(s.product_name) % lower(f.product_name)
WHERE similarity(lower(s.product_name), lower(f.product_name)) > 0.6;




CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX idx_staging_productname_trgm
  ON staging_retailer_events USING gin (lower(product_name) gin_trgm_ops);

CREATE INDEX idx_cleaning_productname_trgm
  ON fashion_coding_cleaning USING gin (lower(product_name) gin_trgm_ops);




select count(*) from staging_normalized sn 
WHERE normalized_product ILIKE '%clothing%'
   OR normalized_product ILIKE '%shoe%'
   OR normalized_product ILIKE '%belt%'
   OR normalized_product ILIKE '%purse%'
   OR normalized_product ILIKE '%bag%'

   OR normalized_product ILIKE '%jewelry%'
   OR normalized_product ILIKE '%dress%'
   OR normalized_product ILIKE '%boots%'
   OR normalized_product ILIKE '%jeans%'
 
   

WITH positives AS (
    SELECT product_name_raw, relevant_code_binary
    FROM fashion_labeled_cleaned
    WHERE relevant_code_binary = 1 AND split_type IS NULL
),
negatives AS (
    SELECT product_name_raw, relevant_code_binary
    FROM fashion_labeled_cleaned
    WHERE relevant_code_binary = 0
      AND split_type IS NULL
      AND (
        lower(product_name_raw) LIKE '%gift card%' OR
        lower(product_name_raw) LIKE '%supplement%' OR
        lower(product_name_raw) LIKE '%incontinence underwear%' OR
        lower(product_name_raw) LIKE '%bag comforter set%' OR
        lower(product_name_raw) LIKE '%pull-out closet drawer large size%' OR
        lower(product_name_raw) LIKE '%curtain%' OR
        lower(product_name_raw) LIKE '%candle%' OR
        lower(product_name_raw) LIKE '%multivitamin%' OR
        lower(product_name_raw) LIKE '%diapers%' OR
        lower(product_name_raw) LIKE '%mainstays%' OR
        lower(product_name_raw) LIKE '%hiking%' OR
        lower(product_name_raw) LIKE '%12 cans%'
      )
    ORDER BY random()
    LIMIT 1748
)
SELECT product_name_raw, relevant_code_binary FROM positives
UNION ALL
SELECT product_name_raw, relevant_code_binary FROM negatives

      