import os
from scripts.etl.db_connection import get_db_connection

def run_sql_migrations():
    conn = get_db_connection()
    if not conn:
        print("Could not connect to the database.")
        return

    cur = conn.cursor()

    # Ensure the tracking table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename TEXT PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()

    # Get applied migrations
    cur.execute("SELECT filename FROM schema_migrations;")
    applied_files = set(row[0] for row in cur.fetchall())

    # Run unapplied migrations in order
    migration_dir = os.path.join("scripts", "migrations", "sql")
    files = sorted(f for f in os.listdir(migration_dir) if f.endswith(".sql"))

    for filename in files:
        if filename in applied_files:
            print(f"Skipping {filename} (already applied)")
            continue

        path = os.path.join(migration_dir, filename)
        print(f"Running migration: {filename}")
        try:
            with open(path, "r") as f:
                sql = f.read()
                cur.execute(sql)
                conn.commit()
                cur.execute("INSERT INTO schema_migrations (filename) VALUES (%s);", (filename,))
                conn.commit()
                print(f"✓ {filename} applied successfully.")
        except Exception as e:
            print(f"✗ Failed to apply {filename}: {e}")
            conn.rollback()

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_sql_migrations()
