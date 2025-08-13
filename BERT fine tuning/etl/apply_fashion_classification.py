import time
from scripts.etl.db_connection import get_db_connection

def apply_fashion_classification(batch_limit=50000, similarity_threshold=0.45):
    """
    Inserts rows into classified_retailer_events by performing a fuzzy join between 
    staging_normalized and retailer_fashion_lookup. Rows are flagged as fashion-related 
    if the similarity between s.normalized_product and l.normalized_name exceeds the threshold.
    
    This script processes rows in batches and prints progress. It uses explicit column 
    mapping (without relying on s.load_timestamp) to avoid tuple index errors.
    """
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database.")
        return
    cur = conn.cursor()

    print("Connected. Beginning classification using fuzzy matching...")

    total_inserted = 0
    while True:
        try:
            start_time = time.time()

            # Use a WITH clause to select a batch of rows from staging_normalized
            # that match a retailer_fashion_lookup row using fuzzy matching.
            # We use DISTINCT ON (s.event_id) to avoid duplicate processing.
            cur.execute("""
                WITH match_batch AS (
                    SELECT DISTINCT ON (s.event_id)
                        s.event_id,
                        s.panelist_id,
                        s.event_name,
                        s.event_type,
                        s.start_time_local,
                        s.end_time_local,
                        s.search_term,
                        s.page_view_id,
                        s.product_id,
                        s.product_name,
                        s.purchase_price,
                        s.purchase_quantity,
                        s.retailer_property_name,
                        s.currency,
                        s.source_file,
                        s.load_status,
                        s.normalized_product,
                        TRUE AS fashion_flag
                    FROM staging_normalized s
                    JOIN retailer_fashion_lookup l
                      ON s.normalized_product % l.normalized_name
                    WHERE similarity(s.normalized_product, l.normalized_name) > %s
                      AND NOT EXISTS (
                        SELECT 1 FROM classified_retailer_events c
                        WHERE c.event_id = s.event_id
                      )
                    ORDER BY s.event_id
                    LIMIT %s
                )
                INSERT INTO classified_retailer_events (
                    event_id, panelist_id, event_name, event_type,
                    start_time_local, end_time_local, search_term, page_view_id,
                    product_id, product_name, purchase_price, purchase_quantity,
                    retailer_property_name, currency, source_file, load_status,
                    normalized_product, fashion_flag
                )
                SELECT 
                    event_id, panelist_id, event_name, event_type,
                    start_time_local, end_time_local, search_term, page_view_id,
                    product_id, product_name, purchase_price, purchase_quantity,
                    retailer_property_name, currency, source_file, load_status,
                    normalized_product, fashion_flag
                FROM match_batch;
            """, (similarity_threshold, batch_limit))

            inserted = cur.rowcount
            conn.commit()
            total_inserted += inserted

            elapsed = round(time.time() - start_time, 2)
            print("Inserted {} rows in {} seconds. Total inserted: {}".format(inserted, elapsed, total_inserted))

            if inserted < batch_limit:
                print("All matching rows processed.")
                break

        except Exception as e:
            conn.rollback()
            print("Error during classification batch: {}".format(e))
            break

    cur.close()
    conn.close()
    print("Fashion classification completed.")

if __name__ == "__main__":
    apply_fashion_classification()
