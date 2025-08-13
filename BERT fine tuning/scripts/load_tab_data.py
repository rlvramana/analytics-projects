import os
import glob
import psycopg2
import csv
from db_connection import get_db_connection  # Make sure this exists and returns a working connection

def load_txt_files_to_staging(data_folder="/Users/ramana/Documents/shopper-analysis/data/tab_data v2"):
    conn = get_db_connection()
    if not conn:
        print("Database connection failed.")
        return

    cur = conn.cursor()
    txt_files = glob.glob(os.path.join(data_folder, "*.txt"))

    for file_path in txt_files:
        filename = os.path.basename(file_path)
        print(f"\nLoading: {filename}")

        with open(file_path, newline='', encoding='utf-8') as f:
            # Strip double quotes from headers and values
            reader = csv.DictReader((line.replace('"', '') for line in f), delimiter='\t')
            print("File headers:", reader.fieldnames)

            row_count = 0
            for row in reader:
                try:
                    if filename == "demos.txt":
                        cur.execute("""
                            INSERT INTO panelist_demographics_p (
                                panelist_id, instance_id, age, postal_code, country, state, dma, dma_code,
                                region, gender, gender_id, ethnicity, ethnicity_id, hispanic, hispanic_id,
                                education, education_id, income, income_id, employment, employment_id,
                                industry, industry_id, occupation, occupation_id, kids_in_household, kids_in_household_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, tuple(row.get(col, None) for col in reader.fieldnames))
                    else:
                        cur.execute("""
                            INSERT INTO staging_retailer_events_p (
                                event_id, panelist_id, event_name, event_type, start_time_local, end_time_local,
                                search_term, page_view_id, product_id, product_name, purchase_price,
                                purchase_quantity, retailer_property_name, currency
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, tuple(row.get(col, None) for col in reader.fieldnames))

                    row_count += 1
                except Exception as e:
                    print(f"Error in {filename} on row {row_count + 1}: {e}")
                    print("Row content:", row)

        print(f"Inserted {row_count} rows from {filename}")

    conn.commit()
    cur.close()
    conn.close()
    print("All files loaded successfully.")

if __name__ == "__main__":
    load_txt_files_to_staging()

