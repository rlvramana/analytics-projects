CREATE TABLE fashion (
    product_name_raw TEXT,
    relevant_code_binary INT
);

CREATE TABLE non_fashion (
    product_name_raw TEXT,
    relevant_code_binary INT
);

INSERT INTO fashion (product_name_raw, relevant_code_binary)
SELECT 
    distinct original_product_name,
    1  -- Setting relevant_code_binary to 1 for all rows
FROM 
    filtered_retailer_events;


INSERT INTO non_fashion (product_name_raw, relevant_code_binary)
SELECT 
    product_name,
    0  -- Set relevant_code_binary to 0 for non-fashion items
FROM 
    staging_retailer_events
WHERE 
    product_name IS NOT NULL
    AND product_name NOT IN (
        SELECT product_name_raw
        FROM fashion
    );


CREATE INDEX idx_fashion_product_name_raw ON fashion(product_name_raw);






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




