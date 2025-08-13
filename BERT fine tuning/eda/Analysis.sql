--these are sqls we used in exploratory data analysis to understand counts,messy data and missing values etc

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

-------------------------------------------------------------------



DO $$
DECLARE
    batch_size INT := 500000;  -- you can adjust batch size
    offset_val INT := 0;
    total_rows INT;
    rows_inserted INT := 0;
BEGIN
    -- Create temp table
    CREATE TEMP TABLE temp_non_fashion (
        product_name TEXT,
        relevant_code_binary INT
    ) ON COMMIT DROP;
    
    -- Count how many distinct non-fashion rows we need to load
    SELECT COUNT(DISTINCT sre.product_name) INTO total_rows
    FROM staging_retailer_events sre
    LEFT JOIN fashion f ON sre.product_name = f.product_name_raw
    WHERE sre.product_name IS NOT NULL
      AND f.product_name_raw IS NULL;

    RAISE NOTICE 'Total distinct non-fashion rows to insert: %', total_rows;
    
    -- Loop in batches
    WHILE offset_val < total_rows LOOP
        -- Insert batch into temp table
        INSERT INTO temp_non_fashion (product_name, relevant_code_binary)
        SELECT DISTINCT
            sre.product_name,
            0
        FROM 
            staging_retailer_events sre
        LEFT JOIN 
            fashion f
        ON 
            sre.product_name = f.product_name_raw
        WHERE 
            sre.product_name IS NOT NULL
            AND f.product_name_raw IS NULL
        ORDER BY sre.product_name
        LIMIT batch_size OFFSET offset_val;
        
        -- Update counters
        offset_val := offset_val + batch_size;
        rows_inserted := rows_inserted + batch_size;
        
        -- Print progress
        RAISE NOTICE 'Inserted ~% rows into temp table so far...', rows_inserted;
    END LOOP;
    
    -- Insert into final table
    INSERT INTO non_fashion (product_name_raw, relevant_code_binary)
    SELECT DISTINCT product_name, relevant_code_binary
    FROM temp_non_fashion;
    
    RAISE NOTICE 'Finished inserting distinct non-fashions into non_fashion table.';
END $$;



WITH fashion_sample AS (
    SELECT 
        product_name_raw,
        relevant_code_binary
    FROM 
        fashion
    ORDER BY RANDOM()
    LIMIT 10000
),
non_fashion_sample AS (
    SELECT 
        product_name_raw,
        relevant_code_binary
    FROM 
        non_fashion
    ORDER BY RANDOM()
    LIMIT 100000
),
combined_dataset AS (
    SELECT * FROM fashion_sample
    UNION ALL
    SELECT * FROM non_fashion_sample
)

SELECT *
FROM combined_dataset
ORDER BY RANDOM();



WITH ones AS (
    SELECT distinct product_name AS product_name_raw, relevant_code AS relevant_code_binary
    FROM fashion_coding_cleaning
    WHERE relevant_code = 1 and product_name <>'NaN'
),
tens AS (
    SELECT distinct product_name AS product_name_raw, relevant_code AS relevant_code_binary
    FROM fashion_coding_cleaning
    WHERE relevant_code in (0,10) and product_name <>'NaN'
 )

SELECT product_name_raw, relevant_code_binary
FROM ones
UNION ALL
SELECT product_name_raw, relevant_code_binary
FROM tens;