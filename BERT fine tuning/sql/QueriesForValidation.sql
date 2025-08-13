
-- 1. Reach: Number of unique panelists per event_type and retailer
SELECT retailer_property_name,
       event_type,
       COUNT(DISTINCT panelist_id) AS unique_users
FROM fact_retailer_events
GROUP BY retailer_property_name, event_type;

-- 2. Engagement Intensity: Events per user per event_type
SELECT retailer_property_name,
       event_type,
       COUNT(event_id) * 1.0 / COUNT(DISTINCT panelist_id) AS events_per_user
FROM fact_retailer_events
GROUP BY retailer_property_name, event_type;

-- 3. Conversion Rate: Checkout / Product Detail per retailer
SELECT retailer_property_name,
       SUM(CASE WHEN event_type = 'Checkout' THEN 1 ELSE 0 END) AS checkout_events,
       SUM(CASE WHEN event_type = 'Product Detail' THEN 1 ELSE 0 END) AS product_detail_events,
       CASE WHEN SUM(CASE WHEN event_type = 'Product Detail' THEN 1 ELSE 0 END) > 0
            THEN SUM(CASE WHEN event_type = 'Checkout' THEN 1 ELSE 0 END) * 1.0 / 
                 SUM(CASE WHEN event_type = 'Product Detail' THEN 1 ELSE 0 END)
       ELSE NULL END AS conversion_rate
FROM fact_retailer_events
GROUP BY retailer_property_name;

-- 4. User Conversion Profiles: Reach, Frequency, Conversion by Brand
SELECT brand,
       COUNT(DISTINCT panelist_id) AS unique_users,
       COUNT(event_id) * 1.0 / COUNT(DISTINCT panelist_id) AS events_per_user,
       SUM(CASE WHEN event_type = 'Checkout' THEN 1 ELSE 0 END) * 1.0 / 
       NULLIF(SUM(CASE WHEN event_type = 'Product Detail' THEN 1 ELSE 0 END), 0) AS conversion_rate
FROM fact_retailer_events
GROUP BY brand;

-- 5. Frequency Threshold Conversion
WITH freq AS (
    SELECT panelist_id,
           COUNT(CASE WHEN event_type = 'Product Detail' THEN 1 END) AS product_detail_count,
           SUM(CASE WHEN event_type = 'Checkout' THEN 1 ELSE 0 END) AS checkout_count
    FROM fact_retailer_events
    GROUP BY panelist_id
)
SELECT CASE 
           WHEN product_detail_count = 1 THEN '1'
           WHEN product_detail_count BETWEEN 2 AND 5 THEN '2-5'
           WHEN product_detail_count BETWEEN 6 AND 10 THEN '6-10'
           ELSE '11+'
       END AS freq_bucket,
       COUNT(*) AS users_in_bucket,
       SUM(CASE WHEN checkout_count > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS conversion_rate
FROM freq
GROUP BY freq_bucket;

-- 6. Segment-Level Conversion Funnel (Fashion only example)
SELECT d.gender,
       d.age,
       COUNT(DISTINCT CASE WHEN f.event_type = 'Product Detail' THEN f.panelist_id END) AS product_detail_users,
       COUNT(DISTINCT CASE WHEN f.event_type = 'Checkout' THEN f.panelist_id END) AS checkout_users,
       COUNT(DISTINCT CASE WHEN f.event_type = 'Checkout' THEN f.panelist_id END) * 1.0 / 
       NULLIF(COUNT(DISTINCT CASE WHEN f.event_type = 'Product Detail' THEN f.panelist_id END), 0) AS conversion_rate
FROM fact_retailer_events f
JOIN dim_panelist d ON f.panelist_id = d.panelist_id
JOIN fashion_coding_cleaning c 
     ON (f.product_name = c.product_name OR f.search_term = c.search_term)
    AND c.relevant_code = 1
GROUP BY d.gender, d.age;

-- 7. Cohort Conversion Over Time
WITH first_touch AS (
    SELECT panelist_id,
           MIN(DATE_TRUNC('month', start_time_local)) AS cohort_month
    FROM fact_retailer_events
    WHERE event_type = 'Product Detail'
    GROUP BY panelist_id
)
SELECT ft.cohort_month,
       DATE_TRUNC('month', f.start_time_local) AS activity_month,
       COUNT(DISTINCT CASE WHEN f.event_type = 'Product Detail' THEN f.panelist_id END) AS product_detail_users,
       COUNT(DISTINCT CASE WHEN f.event_type = 'Checkout' THEN f.panelist_id END) AS checkout_users
FROM first_touch ft
JOIN fact_retailer_events f ON ft.panelist_id = f.panelist_id
GROUP BY ft.cohort_month, activity_month
ORDER BY ft.cohort_month, activity_month;


-- Query 1: Distinct counts of each event_type and checkout rate
SELECT
    e.event_type,
    COUNT(DISTINCT e.event_id) AS distinct_event_count,
    COUNT(DISTINCT CASE WHEN e.event_type = 'Checkout' THEN e.event_id END) AS distinct_checkouts,
    COUNT(DISTINCT CASE WHEN e.event_type = 'Product Detail' THEN e.event_id END) AS distinct_product_details,
    ROUND(
        COUNT(DISTINCT CASE WHEN e.event_type = 'Checkout' THEN e.event_id END)::decimal
        /
        NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'Product Detail' THEN e.event_id END), 0),
        4
    ) AS checkout_rate
FROM fashion_events_dep_final e
JOIN dim_panelist p
    ON e.panelist_id = p.panelist_id
GROUP BY e.event_type
ORDER BY e.event_type;

-- Query 2: Distinct event counts by event_type
SELECT
    e.event_type,
    COUNT(DISTINCT e.event_id) AS distinct_event_count
FROM fashion_events_dep_final e
JOIN dim_panelist p
    ON e.panelist_id = p.panelist_id
GROUP BY e.event_type
ORDER BY distinct_event_count DESC;

-- Query 3: Crosstab event counts for each retailer
SELECT
    e.retailer_property_name,
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
ORDER BY e.retailer_property_name;

-- Query 4: Product names in first table but not in second
SELECT product_name
FROM staging_retailer_events
WHERE product_name NOT IN (
    SELECT product_name FROM fashion_events_dep_final
);

-- Query 5: Count of distinct product names
SELECT COUNT(DISTINCT product_name) FROM staging_retailer_events;

SELECT COUNT(DISTINCT product_name) FROM fashion_events_dep_final;


-- Count rows in each split type
SELECT split_type, COUNT(*) AS row_count
FROM fashion_labeled_cleaned
GROUP BY split_type;

-- Count rows where product_name_raw is NULL but relevant_code_binary has a value
SELECT COUNT(*) AS null_product_name_count
FROM fashion_labeled_cleaned
WHERE product_name_raw IS NULL AND relevant_code_binary IS NOT NULL;

-- Pull positives and limited negatives for training/validation
WITH positives AS (
    SELECT *
    FROM fashion_labeled_cleaned
    WHERE relevant_code_binary = 1 AND split_type IS NULL
),
negatives AS (
    SELECT *
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
SELECT product_name_raw, relevant_code_binary FROM negatives;


-- Check counts in both tables
SELECT COUNT(*) FROM staging_retailer_events;
SELECT COUNT(*) FROM fashion_coding_cleaning;

-- Create intermediate table by joining on product_name
CREATE TABLE cleaned_retailer_events AS
SELECT 
    s.event_id,
    s.panelist_id,
    s.event_type,
    s.start_time_local,
    s.end_time_local,
    s.search_term AS search_term_raw,
    NULL AS search_term_clean,
    s.product_id,
    s.product_name AS product_name_raw,
    NULL AS product_name_clean,
    TRY_CAST(s.purchase_price AS NUMERIC) AS purchase_price,
    TRY_CAST(s.purchase_quantity AS INTEGER) AS purchase_quantity,
    s.retailer_property_name,
    s.currency,
    f.relevant_code,
    NULL AS taxonomy_level_1,
    NULL AS taxonomy_level_2,
    NULL AS taxonomy_level_3,
    NULL AS taxonomy_level_4,
    NULL AS category_source,
    NULL AS label_confidence
FROM staging_retailer_events s
LEFT JOIN fashion_coding_cleaning f
  ON s.product_name = f.product_name;

-- Sample data check after join
SELECT * FROM cleaned_retailer_events LIMIT 100;

