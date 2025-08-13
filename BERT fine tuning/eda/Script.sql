

CREATE TABLE fashion_events_final (
    event_id TEXT,
    panelist_id TEXT,
    event_name TEXT,
    event_type TEXT,
    start_time_local TIMESTAMP,
    end_time_local TIMESTAMP,
    search_term TEXT,
    product_id TEXT,
    product_name TEXT,
    purchase_price NUMERIC,
    purchase_price_raw TEXT,
    purchase_price_invalid BOOLEAN,
    purchase_quantity INTEGER,
    purchase_quantity_raw TEXT,
    purchase_quantity_invalid BOOLEAN,
    retailer_property_name TEXT,
    currency TEXT,
    age INTEGER
);




INSERT INTO fashion_events_final(
    event_id,
    panelist_id,
    event_name,
    event_type,
    start_time_local,
    end_time_local,
    search_term,
    product_id,
    product_name,
    purchase_price,
    purchase_price_raw,
    purchase_price_invalid,
    purchase_quantity,
    purchase_quantity_raw,
    purchase_quantity_invalid,
    retailer_property_name,
    currency,
    age
)
SELECT
    event_id,
    panelist_id,
    event_name,
    event_type,
    start_time_local::timestamp,
    end_time_local::timestamp,
    search_term,
    product_id,
    product_name,

    -- Purchase price: valid numeric or NULL
    CASE
        WHEN purchase_price ~ '^[0-9]+(\.[0-9]+)?$' THEN purchase_price::numeric
        ELSE NULL
    END AS purchase_price,

    -- Raw price string
    purchase_price AS purchase_price_raw,

    -- Invalid price flag
    CASE
        WHEN purchase_price ~ '^[0-9]+(\.[0-9]+)?$' THEN FALSE
        ELSE TRUE
    END AS purchase_price_invalid,

    -- Purchase quantity: valid integer or NULL
    CASE
        WHEN purchase_quantity ~ '^[0-9]+$' THEN purchase_quantity::integer
        ELSE NULL
    END AS purchase_quantity,

    -- Raw quantity string
    purchase_quantity AS purchase_quantity_raw,

    -- Invalid quantity flag
    CASE
        WHEN purchase_quantity ~ '^[0-9]+$' THEN FALSE
        ELSE TRUE
    END AS purchase_quantity_invalid,

    retailer_property_name,
    currency,

    -- Age: same safe integer casting
    CASE
        WHEN age ~ '^[0-9]+$' THEN age::integer
        ELSE NULL
    END AS age

FROM fashion_events;


--------------------------------------------------------------------------------

CREATE TABLE fashion_events_dep_final (
    event_id TEXT,
    panelist_id TEXT,
    event_name TEXT,
    event_type TEXT,
    start_time_local TIMESTAMP,
    end_time_local TIMESTAMP,
    search_term TEXT,
    product_id TEXT,
    product_name TEXT,
    purchase_price NUMERIC,
    purchase_price_raw TEXT,
    purchase_price_invalid BOOLEAN,
    purchase_quantity INTEGER,
    purchase_quantity_raw TEXT,
    purchase_quantity_invalid BOOLEAN,
    retailer_property_name TEXT,
    currency TEXT,
    age INTEGER,
    department TEXT
);


INSERT INTO fashion_events_dep_final (
    event_id,
    panelist_id,
    event_name,
    event_type,
    start_time_local,
    end_time_local,
    search_term,
    product_id,
    product_name,
    purchase_price,
    purchase_price_raw,
    purchase_price_invalid,
    purchase_quantity,
    purchase_quantity_raw,
    purchase_quantity_invalid,
    retailer_property_name,
    currency,
    age,
    department
)
SELECT
    event_id,
    panelist_id,
    event_name,
    event_type,
    start_time_local::timestamp,
    end_time_local::timestamp,
    search_term,
    product_id,
    product_name,

    -- Purchase price: valid numeric or NULL
    CASE
        WHEN purchase_price ~ '^[0-9]+(\.[0-9]+)?$' THEN purchase_price::numeric
        ELSE NULL
    END AS purchase_price,

    -- Raw price string
    purchase_price AS purchase_price_raw,

    -- Invalid price flag
    CASE
        WHEN purchase_price ~ '^[0-9]+(\.[0-9]+)?$' THEN FALSE
        ELSE TRUE
    END AS purchase_price_invalid,

    -- Purchase quantity: valid integer or NULL
    CASE
        WHEN purchase_quantity ~ '^[0-9]+$' THEN purchase_quantity::integer
        ELSE NULL
    END AS purchase_quantity,

    -- Raw quantity string
    purchase_quantity AS purchase_quantity_raw,

    -- Invalid quantity flag
    CASE
        WHEN purchase_quantity ~ '^[0-9]+$' THEN FALSE
        ELSE TRUE
    END AS purchase_quantity_invalid,

    retailer_property_name,
    currency,

    -- Age: valid integer or NULL
    CASE
        WHEN age ~ '^[0-9]+$' THEN age::integer
        ELSE NULL
    END AS age,

    department

FROM fashion_events_dep;


UPDATE cleaned_retailer_events cre
SET relevant_code_binary = flc.relevant_code_binary
FROM fashion_labeled_cleaned flc
WHERE cre.product_name_raw = flc.product_name_raw
  AND cre.relevant_code_binary IS NULL
  AND flc.relevant_code_binary IS NOT NULL;

