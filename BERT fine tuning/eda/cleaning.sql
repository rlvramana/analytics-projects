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
  NULL AS product_name_clean)

  -- Cleaned purchase_price: only clean numbers
  CASE
    WHEN TRIM(s.purchase_price) = '' THEN NULL
    WHEN REGEXP_REPLACE(s.purchase_price, '[^\d\.]', '', 'g') ~ '^[0-9]+(\.[0-9]+)?$'
      THEN CAST(REGEXP_REPLACE(s.purchase_price, '[^\d\.]', '', 'g') AS NUMERIC)
    ELSE NULL
  END AS purchase_price,

  -- Raw bad purchase_price (only stored if invalid)
  CASE
    WHEN TRIM(s.purchase_price) = '' THEN NULL
    WHEN REGEXP_REPLACE(s.purchase_price, '[^\d\.]', '', 'g') ~ '^[0-9]+(\.[0-9]+)?$'
      THEN NULL
    ELSE s.purchase_price
  END AS raw_bad_purchase_price,

  -- Flag: was purchase_price bad?
  CASE
    WHEN TRIM(s.purchase_price) = '' THEN FALSE
    WHEN REGEXP_REPLACE(s.purchase_price, '[^\d\.]', '', 'g') ~ '^[0-9]+(\.[0-9]+)?$'
      THEN FALSE
    ELSE TRUE
  END AS bad_purchase_price,

  -- Cleaned purchase_quantity: only clean integers
  CASE
    WHEN TRIM(s.purchase_quantity) = '' THEN NULL
    WHEN s.purchase_quantity ~ '^[0-9]+$'
      THEN CAST(s.purchase_quantity AS INTEGER)
    ELSE NULL
  END AS purchase_quantity,

  -- Raw bad purchase_quantity (only stored if invalid)
  CASE
    WHEN TRIM(s.purchase_quantity) = '' THEN NULL
    WHEN s.purchase_quantity ~ '^[0-9]+$'
      THEN NULL
    ELSE s.purchase_quantity
  END AS raw_bad_purchase_quantity,

  -- Flag: was purchase_quantity bad?
  CASE
    WHEN TRIM(s.purchase_quantity) = '' THEN FALSE
    WHEN s.purchase_quantity ~ '^[0-9]+$'
      THEN FALSE
    ELSE TRUE
  END AS bad_purchase_quantity,

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



CREATE TABLE shopper_events (
    event_id TEXT,
    panelist_id TEXT,
    event_name TEXT,
    event_type TEXT,
    start_time_local TEXT,
    end_time_local TEXT,
    search_term TEXT,
    product_id TEXT,
    product_name TEXT,
    purchase_price TEXT,
    purchase_quantity TEXT,
    retailer_property_name TEXT,
    currency TEXT,
    age TEXT
);

CREATE TABLE shopper_events_product_name (
    event_id TEXT,
    panelist_id TEXT,
    event_name TEXT,
    event_type TEXT,
    start_time_local TEXT,
    end_time_local TEXT,
    search_term TEXT,
    product_id TEXT,
    product_name TEXT,
    purchase_price TEXT,
    purchase_quantity TEXT,
    retailer_property_name TEXT,
    currency TEXT,
    age TEXT,
    product_name_uncleaned TEXT
);
