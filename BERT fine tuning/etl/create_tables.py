from .db_connection import get_db_connection

def create_tables():
    """Creates necessary tables in the database."""
    conn = get_db_connection()
    if not conn:
        print("Failed to connect. Tables not created.")
        return

    cur = conn.cursor()

    # 1) Staging table (holds raw data)
    cur.execute("""
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
    """)

    # 2) Log table for tracking loaded files
    cur.execute("""
    CREATE TABLE IF NOT EXISTS load_log (
        id SERIAL PRIMARY KEY,
        file_name TEXT UNIQUE,
        load_timestamp TIMESTAMP DEFAULT NOW(),
        total_rows INT,
        loaded_rows INT,
        status TEXT
    );
    """)


    # 3) Final classified table (for ML results)
    cur.execute("""
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
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Tables created successfully.")

if __name__ == "__main__":
    create_tables()
