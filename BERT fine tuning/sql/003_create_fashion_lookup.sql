-- 003_create_fashion_lookup.sql

CREATE TABLE IF NOT EXISTS retailer_fashion_lookup (
    retailer TEXT,                 -- e.g. 'Amazon', 'Walmart'
    meta_product_id TEXT,          -- product ID from the source
    meta_product_name TEXT,        -- original product name
    brand TEXT,                    -- optional brand field
    category TEXT,                 -- high-level category (if available)
    normalized_name TEXT,          -- lowercased, cleaned version of product name
    raw_data JSONB,                -- optional: store the full original JSON
    PRIMARY KEY (retailer, meta_product_id)
);

-- Ensure pg_trgm is enabled for fuzzy text matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create GIN index on normalized_name for fuzzy joins
CREATE INDEX IF NOT EXISTS idx_retailer_fashion_lookup_norm
ON retailer_fashion_lookup USING gin (normalized_name gin_trgm_ops);
