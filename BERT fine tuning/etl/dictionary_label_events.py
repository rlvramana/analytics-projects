from .db_connection import get_db_connection
import re
import time

def dictionary_label_events(batch_size=50000, max_retries=3):
    """
    Reads from staging_retailer_events in batches using a server-side (named) cursor
    on one connection, applies dictionary-based fashion keyword matching (covering clothing,
    shoes, belts, purses/bags, jewelry, etc.) to label each row,
    and inserts the results into dictionary_labeled_events using a separate connection.
    
    It processes data in batches to handle large volumes without exhausting memory,
    and uses a retry mechanism on the write connection to handle transient failures.
    
    Parameters:
      batch_size (int): Number of rows to process per batch. You can increase this
                        from the default 10,000; here we use 20,000 as an example.
      max_retries (int): Number of times to retry a batch if a commit fails.
    """
    count = 0
    # Connection for reading (named cursor)
    read_conn = get_db_connection()
    if not read_conn:
        print("Failed to connect for reading.")
        return
    read_cur = read_conn.cursor(name="staging_cursor")
    read_cur.itersize = batch_size
    read_cur.execute("""
        SELECT event_id, panelist_id, event_name, event_type, 
               start_time_local, end_time_local, search_term, page_view_id,
               product_id, product_name, purchase_price, purchase_quantity,
               retailer_property_name, currency, source_file, load_status
        FROM staging_retailer_events;
    """)

    # Separate connection for inserts
    write_conn = get_db_connection()
    if not write_conn:
        print("Failed to connect for writing.")
        read_cur.close()
        read_conn.close()
        return
    write_cur = write_conn.cursor()

    # Comprehensive list of fashion-related keywords
    fashion_keywords = [
        "shirt", "t-shirt", "tee", "dress", "skirt", "pants", "jeans", "shorts",
        "jacket", "coat", "sweater", "hoodie", "blouse", "cardigan", "suit", "tie", 
        "belt", "underwear", "lingerie", "sock", "shoe", "sneaker", "boot", "sandal", 
        "purse", "bag", "handbag", "backpack", "wallet", "jewelry", "necklace", "earring", 
        "bracelet", "ring", "watch", "glasses", "sunglasses", "hat", "cap", "scarf", 
        "glove", "beanie", "trousers", "activewear", "swimwear", "intimates", "accessory", 
        "footwear"
    ]
    pattern = re.compile(r"\b(" + "|".join(fashion_keywords) + r")\b", re.IGNORECASE)

    insert_sql = """
        INSERT INTO dictionary_labeled_events (
            event_id, panelist_id, event_name, event_type,
            start_time_local, end_time_local, search_term, page_view_id,
            product_id, product_name, purchase_price, purchase_quantity,
            retailer_property_name, currency, source_file, load_status,
            dictionary_label
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    while True:
        rows = read_cur.fetchmany(batch_size)
        if not rows:
            break
        retry_count = 0
        while retry_count < max_retries:
            try:
                for row in rows:
                    (event_id, panelist_id, event_name, event_type,
                     start_time_local, end_time_local, search_term, page_view_id,
                     product_id, product_name, purchase_price, purchase_quantity,
                     retailer_property_name, currency, source_file, load_status) = row

                    combined_text = (product_name or "") + " " + (search_term or "")
                    is_fashion = bool(pattern.search(combined_text))

                    write_cur.execute(insert_sql, (
                        event_id, panelist_id, event_name, event_type,
                        start_time_local, end_time_local, search_term, page_view_id,
                        product_id, product_name, purchase_price, purchase_quantity,
                        retailer_property_name, currency, source_file, load_status,
                        is_fashion
                    ))
                    count += 1
                write_conn.commit()
                print("Processed {} rows...".format(count))
                break  # Successful commit; break out of retry loop.
            except Exception as e:
                write_conn.rollback()
                retry_count += 1
                print("Error committing batch, retry {}/{}: {}".format(retry_count, max_retries, e))
                time.sleep(2)  # Wait a bit before retrying.
        if retry_count == max_retries:
            print("Batch failed after {} retries; skipping this batch.".format(max_retries))
        # Continue to next batch

    # Final commit (if any remains) and cleanup.
    try:
        write_conn.commit()
    except Exception as final_commit_err:
        print("Error in final commit: ", final_commit_err)
    try:
        write_cur.close()
    except Exception as e:
        print("Error closing write cursor: ", e)
    try:
        write_conn.close()
    except Exception as e:
        print("Error closing write connection: ", e)
    try:
        read_cur.close()
    except Exception as e:
        print("Error closing read cursor: ", e)
    try:
        read_conn.close()
    except Exception as e:
        print("Error closing read connection: ", e)
    print("Finished labeling {} rows in dictionary_labeled_events.".format(count))

if __name__ == "__main__":
    dictionary_label_events()
