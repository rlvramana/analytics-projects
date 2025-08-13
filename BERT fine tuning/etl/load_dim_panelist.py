from scripts.etl.db_connection import get_db_connection

BATCH_SIZE = 1000
EXPECTED_COLUMNS = 27  # Number of columns to insert

def load_dim_panelist():
    """
    Loads unique panelist records from panelist_demographics into dim_panelist.
    Handles batch inserts, avoids duplicates, and ensures correct column matching.
    """
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to database.")
        return

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT DISTINCT
                panelist_id, instance_id, age, postal_code, country, state, dma, dma_code, region,
                gender, gender_id, ethnicity, ethnicity_id, hispanic, hispanic_id,
                education, education_id, income, income_id, employment, employment_id,
                industry, industry_id, occupation, occupation_id, kids_in_household, kids_in_household_id
            FROM panelist_demographics
            WHERE panelist_id IS NOT NULL;
        """)
        rows = cur.fetchall()
        total = len(rows)
        print(f"Found {total} panelist records.")

        batch = []
        inserted = 0
        for row in rows:
            # Ensure the row has exactly 27 columns
            row_tuple = tuple(row) if len(row) == EXPECTED_COLUMNS else tuple(
                row[i] if i < len(row) else None for i in range(EXPECTED_COLUMNS)
            )
            batch.append(row_tuple)

            if len(batch) >= BATCH_SIZE:
                cur.executemany("""
                    INSERT INTO dim_panelist (
                        panelist_id, instance_id, age, postal_code, country, state, dma, dma_code, region,
                        gender, gender_id, ethnicity, ethnicity_id, hispanic, hispanic_id,
                        education, education_id, income, income_id, employment, employment_id,
                        industry, industry_id, occupation, occupation_id, kids_in_household, kids_in_household_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (panelist_id) DO NOTHING;
                """, batch)
                conn.commit()
                inserted += len(batch)
                print(f"Inserted {inserted} records so far...")
                batch = []

        if batch:
            cur.executemany("""
                INSERT INTO dim_panelist (
                    panelist_id, instance_id, age, postal_code, country, state, dma, dma_code, region,
                    gender, gender_id, ethnicity, ethnicity_id, hispanic, hispanic_id,
                    education, education_id, income, income_id, employment, employment_id,
                    industry, industry_id, occupation, occupation_id, kids_in_household, kids_in_household_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s)
                ON CONFLICT (panelist_id) DO NOTHING;
            """, batch)
            conn.commit()
            inserted += len(batch)
            print(f"Inserted final {len(batch)} records. Total: {inserted}")

        print("✓ dim_panelist load complete.")
    except Exception as e:
        conn.rollback()
        print("✗ Error loading dim_panelist:", e)
    finally:
        try:
            cur.close()
        except Exception as e:
            print("Warning: Failed to close cursor:", e)
        conn.close()

if __name__ == "__main__":
    load_dim_panelist()
