import json
import os
from scripts.etl.db_connection import get_db_connection

DEMOS_FILE = "data/demos/demos_full000.json"  # Adjust path/filename as needed
BATCH_SIZE = 50000

def load_demographics():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to the database.")
        return
    cur = conn.cursor()

    if not os.path.exists(DEMOS_FILE):
        print(f"File not found: {DEMOS_FILE}")
        return

    batch = []
    count = 0

    with open(DEMOS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line.strip())
                panelist_id = row.get("panelist_id")
                instance_id = row.get("instance_id")
                age = row.get("age")
                postal_code = row.get("postal_code")
                country = row.get("country")
                state = row.get("state")
                dma = row.get("dma")
                dma_code = row.get("dma_code")
                region = row.get("region")
                gender = row.get("gender")
                gender_id = row.get("gender_id")
                ethnicity = row.get("ethnicity")
                ethnicity_id = row.get("ethnicity_id")
                hispanic = row.get("hispanic")
                hispanic_id = row.get("hispanic_id")
                education = row.get("education")
                education_id = row.get("education_id")
                income = row.get("income")
                income_id = row.get("income_id")
                employment = row.get("employment")
                employment_id = row.get("employment_id")
                industry = row.get("industry")
                industry_id = row.get("industry_id")
                occupation = row.get("occupation")
                occupation_id = row.get("occupation_id")
                kids_in_household = row.get("kids_in_household")
                kids_in_household_id = row.get("kids_in_household_id")

                # You can add any checks, e.g. skip if panelist_id missing
                if not panelist_id:
                    continue

                batch.append((
                    panelist_id,
                    instance_id,
                    age,
                    postal_code,
                    country,
                    state,
                    dma,
                    dma_code,
                    region,
                    gender,
                    gender_id,
                    ethnicity,
                    ethnicity_id,
                    hispanic,
                    hispanic_id,
                    education,
                    education_id,
                    income,
                    income_id,
                    employment,
                    employment_id,
                    industry,
                    industry_id,
                    occupation,
                    occupation_id,
                    kids_in_household,
                    kids_in_household_id
                ))

                if len(batch) >= BATCH_SIZE:
                    cur.executemany("""
                        INSERT INTO panelist_demographics (
                            panelist_id, instance_id, age, postal_code, country,
                            state, dma, dma_code, region, gender, gender_id,
                            ethnicity, ethnicity_id, hispanic, hispanic_id,
                            education, education_id, income, income_id,
                            employment, employment_id, industry, industry_id,
                            occupation, occupation_id, kids_in_household,
                            kids_in_household_id
                        )
                        VALUES (%s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s)
                    """, batch)
                    conn.commit()
                    count += len(batch)
                    print(f"Inserted {count} rows so far.")
                    batch = []

            except Exception as e:
                print("Skipping line due to error:", e)
                continue

    # Insert any remaining rows
    if batch:
        cur.executemany("""
            INSERT INTO panelist_demographics (
                panelist_id, instance_id, age, postal_code, country,
                state, dma, dma_code, region, gender, gender_id,
                ethnicity, ethnicity_id, hispanic, hispanic_id,
                education, education_id, income, income_id,
                employment, employment_id, industry, industry_id,
                occupation, occupation_id, kids_in_household,
                kids_in_household_id
            )
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s)
        """, batch)
        conn.commit()
        count += len(batch)
        print(f"Inserted final {len(batch)} rows...")

    print(f"Finished loading {count} demographic rows.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_demographics()
