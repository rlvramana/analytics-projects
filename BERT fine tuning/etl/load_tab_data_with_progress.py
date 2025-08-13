import psycopg2
import glob
import os

# Adjust connection details as needed
conn = psycopg2.connect(
    dbname='',
    user='',
    password='',
    host='',     
    port=''      
)
conn.autocommit = False  # We'll commit once at the end
cur = conn.cursor()

folder_path = '/path/to/tab_data_v2'  # <--- adjust to your actual path

# Look for files matching the pattern export_shopper=*.txt
file_pattern = os.path.join(folder_path, 'export_shopper=*.txt')
text_files = glob.glob(file_pattern)

for file_path in text_files:
    print(f'Loading file: {file_path}')
    with open(file_path, 'r', encoding='utf-8') as f:
        # The COPY command must match your file format: 
        #    1) Are columns delimited by tabs? 
        #    2) Is there a header line?
        #    3) Are values quoted or not?

        # Typically for tab-delimited with a header row, you might do:
        copy_sql = """
            COPY staging_retailer_events_p 
            FROM STDIN 
            WITH (
                FORMAT csv,
                DELIMITER E'\t',
                HEADER,
                QUOTE '\"',
                ESCAPE '\"'
            )
        """
        cur.copy_expert(copy_sql, f)

# Commit changes after loading all files
conn.commit()
cur.close()
conn.close()

print("All files loaded successfully.")
