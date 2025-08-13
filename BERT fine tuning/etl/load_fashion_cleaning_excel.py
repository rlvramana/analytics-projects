import pandas as pd
import psycopg2
from scripts.etl.db_connection import get_db_connection

FILE_PATH = "data/SHARED MSU FASHION/google_fashion_retailer_cleaning_final - FOR MSU.xlsm"
SHEET_NAME = "Fashion Coding"

def load_excel_to_postgres():
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to the database.")
        return

    try:
        df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME)

        df = df.rename(columns={
            "search_term": "search_term",
            "product_name": "product_name",
            "count": "count",
            "relevant_code": "relevant_code"
        })

        cur = conn.cursor()

        insert_query = """
            INSERT INTO fashion_coding_cleaning (
                search_term, product_name, count, relevant_code
            ) VALUES (%s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            cur.execute(insert_query, (
                row.get('search_term'),
                row.get('product_name'),
                int(row.get('count')) if not pd.isna(row.get('count')) else None,
                int(row.get('relevant_code')) if not pd.isna(row.get('relevant_code')) else None
            ))

        conn.commit()
        print(f"Inserted {len(df)} rows into fashion_coding_cleaning.")
        cur.close()
    except Exception as e:
        print("Error during insert:", e)
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    load_excel_to_postgres()
