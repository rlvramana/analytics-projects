-- 002_create_staging_normalized.sql

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS staging_normalized AS
SELECT *,
       lower(trim(product_name || ' ' || search_term)) AS normalized_product
FROM staging_retailer_events
WHERE product_name IS NOT NULL OR search_term IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_staging_normalized_product
ON staging_normalized USING gin (normalized_product gin_trgm_ops);
