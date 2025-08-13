from .db_connection import get_db_connection
import json
import glob
import os
from datetime import datetime

def load_json_to_staging(data_folder="data"):
    """
    Recursively reads JSON files (including subfolders) from data_folder,
    inserts rows into staging_retailer_events, and records file activity
    in load_log to avoid duplicates.
    """
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to the database.")
        return

    cur = conn.cursor()

    # Recursively find all .json files in subfolders
    json_files = glob.glob(os.path.join(data_folder, "**/*.json"), recursive=True)

    for file_path in json_files:
        # Compute the relative path from data_folder (e.g., export_shopper=AUG-24/0000_part_00.json)
        relative_path = os.path.relpath(file_path, data_folder)

        # Check if this file was already loaded by looking in load_log
        cur.execute("SELECT COUNT(*) FROM load_log WHERE file_name = %s", (relative_path,))
        already_loaded = cur.fetchone()[0] > 0

        if already_loaded:
            print(f"Skipping {relative_path} (already loaded).")
            continue

        total_rows = 0
        loaded_rows = 0

        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                total_rows += 1
                try:
                    row = json.loads(line.strip())
                    cur.execute("""
                        INSERT INTO staging_retailer_events (
                            event_id,
                            panelist_id,
                            event_name,
                            event_type,
                            start_time_local,
                            end_time_local,
                            search_term,
                            page_view_id,
                            product_id,
                            product_name,
                            purchase_price,
                            purchase_quantity,
                            retailer_property_name,
                            currency,
                            source_file,
                            load_status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row.get("event_id"),
                        row.get("panelist_id"),
                        row.get("event_name"),
                        row.get("event_type"),
                        row.get("start_time_local"),
                        row.get("end_time_local"),
                        row.get("search_term"),
                        row.get("page_view_id"),
                        row.get("product_id"),
                        row.get("product_name"),
                        row.get("purchase_price"),
                        row.get("purchase_quantity"),
                        row.get("retailer_property_name"),
                        row.get("currency"),
                        relative_path,
                        "success"
                    ))
                    loaded_rows += 1
                except Exception as e:
                    print(f"Error loading row in {relative_path}: {e}")

        # Log the load attempt in load_log table
        cur.execute("""
            INSERT INTO load_log (file_name, total_rows, loaded_rows, status)
            VALUES (%s, %s, %s, %s)
        """, (
            relative_path,
            total_rows,
            loaded_rows,
            "success" if total_rows == loaded_rows else "partial"
        ))

        print(f"File: {relative_path}, Total Rows: {total_rows}, Loaded: {loaded_rows}")

    conn.commit()
    cur.close()
    conn.close()
    print("Loading process completed.")

if __name__ == "__main__":
    load_json_to_staging("data")

