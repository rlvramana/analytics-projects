CREATE TABLE IF NOT EXISTS bad_fact_events (
    id SERIAL PRIMARY KEY,
    event_id TEXT,
    source_file TEXT,
    purchase_price TEXT,
    raw_data JSONB,
    error_reason TEXT,
    logged_at TIMESTAMP DEFAULT now()
);
