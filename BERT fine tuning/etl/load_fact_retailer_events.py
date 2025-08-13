import json
from scripts.etl.db_connection import get_db_connection

BATCH_SIZE = 50000

def load_fact_retailer_events():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database.")
        return

    cur = conn.cursor()
    try:
        print("Connected to PostgreSQL!")

        # Count rows for visibility
        cur.execute("SELECT COUNT(*) FROM staging_retailer_events;")
        total_staging = cur.fetchone()[0]
        print(f"Found {total_staging} rows in staging.")

        cur.execute("SELECT COUNT(*) FROM fact_retailer_events;")
        existing = cur.fetchone()[0]
        print(f"Found {existing} rows in fact_retailer_events.")

        # Stream data with LEFT JOIN to avoid reloading
        cur.execute("""
            SELECT sre.event_id, sre.panelist_id, sre.event_name, sre.event_type,
                   sre.start_time_local, sre.end_time_local,
                   sre.search_term, sre.page_view_id, sre.product_id, sre.product_name,
                   sre.purchase_price, sre.purchase_quantity,
                   sre.retailer_property_name, sre.currency,
                   sre.source_file, sre.load_status
            FROM staging_retailer_events sre
            LEFT JOIN fact_retailer_events fre ON sre.event_id = fre.event_id
            WHERE fre.event_id IS NULL;
        """)
        rows = cur.fetchall()

        insert_sql = """
            INSERT INTO fact_retailer_events (
                event_id, panelist_id, event_name, event_type,
                start_time_local, end_time_local,
                search_term, page_view_id, product_id, product_name,
                purchase_price, purchase_quantity,
                retailer_property_name, currency,
                source_file, load_status,
                brand, product_category
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, NULL, NULL);
        """

        quarantine_sql = """
            INSERT INTO bad_fact_events (
                event_id, source_file, purchase_price, raw_data, error_reason
            ) VALUES (%s, %s, %s, %s, %s);
        """

        batch = []
        quarantined = 0
        inserted = 0

        for row in rows:
            try:
                event_id, panelist_id, event_name, event_type, \
                start_time_local, end_time_local, \
                search_term, page_view_id, product_id, product_name, \
                purchase_price_raw, purchase_quantity_raw, \
                retailer_property_name, currency, source_file, load_status = row

                # Sanitize price and quantity
                purchase_price = float(purchase_price_raw) if purchase_price_raw and purchase_price_raw.replace('.', '', 1).isdigit() else None
                purchase_quantity = int(purchase_quantity_raw) if purchase_quantity_raw and purchase_quantity_raw.isdigit() else None

                batch.append((
                    event_id, panelist_id, event_name, event_type,
                    start_time_local, end_time_local,
                    search_term, page_view_id, product_id, product_name,
                    purchase_price, purchase_quantity,
                    retailer_property_name, currency,
                    source_file, load_status
                ))

            except Exception as e:
                quarantined += 1
                raw_data = {
                    "event_id": event_id,
                    "product_name": product_name,
                    "purchase_price": purchase_price_raw,
                    "purchase_quantity": purchase_quantity_raw
                }
                cur.execute(quarantine_sql, (
                    event_id, source_file, str(purchase_price_raw),
                    json.dumps(raw_data), str(e)
                ))

            if len(batch) >= BATCH_SIZE:
                cur.executemany(insert_sql, batch)
                conn.commit()
                inserted += len(batch)
                print(f"✓ Inserted {inserted} rows so far...")
                batch.clear()

        if batch:
            cur.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)
            print(f"✓ Inserted final {len(batch)} rows. Total inserted: {inserted}")

        print(f"✓ Load complete. {quarantined} rows quarantined due to errors.")

    except Exception as e:
        conn.rollback()
        print("✗ Error during fact load:", e)
    finally:
        try:
            cur.close()
        except:
            pass
        conn.close()

if __name__ == "__main__":
    load_fact_retailer_events()
