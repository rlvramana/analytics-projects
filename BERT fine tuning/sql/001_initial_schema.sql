-- scripts/migrations/sql/001_initial_schema.sql

CREATE TABLE IF NOT EXISTS staging_retailer_events (
    event_id TEXT,
    panelist_id TEXT,
    event_name TEXT,
    event_type TEXT,
    start_time_local TEXT,
    end_time_local TEXT,
    search_term TEXT,
    page_view_id TEXT,
    product_id TEXT,
    product_name TEXT,
    purchase_price TEXT,
    purchase_quantity TEXT,
    retailer_property_name TEXT,
    currency TEXT,
    load_timestamp TIMESTAMP DEFAULT NOW(),
    source_file TEXT,
    load_status TEXT
);

CREATE TABLE IF NOT EXISTS load_log (
    id SERIAL PRIMARY KEY,
    file_name TEXT UNIQUE,
    load_timestamp TIMESTAMP DEFAULT NOW(),
    total_rows INT,
    loaded_rows INT,
    status TEXT
);

CREATE TABLE IF NOT EXISTS classified_retailer_events (
    event_id TEXT,
    panelist_id TEXT,
    event_name TEXT,
    event_type TEXT,
    start_time_local TEXT,
    end_time_local TEXT,
    search_term TEXT,
    page_view_id TEXT,
    product_id TEXT,
    product_name TEXT,
    purchase_price TEXT,
    purchase_quantity TEXT,
    retailer_property_name TEXT,
    currency TEXT,
    source_file TEXT,
    load_status TEXT,
    clothing_flag BOOLEAN,
    classified_timestamp TIMESTAMP DEFAULT NOW()
);