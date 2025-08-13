import psycopg2
import mysql.connector
from configparser import ConfigParser

def get_db_connection():
    """Reads config.ini and returns a database connection."""
    
    # Load configuration file
    config = ConfigParser()
    config.read("config/config.ini")  # Adjust path if needed

    DB_TYPE = config["database"]["db_type"]
    DB_HOST = config["database"]["host"]
    DB_NAME = config["database"]["dbname"]
    DB_USER = config["database"]["user"]
    DB_PASS = config["database"]["password"]
    DB_PORT = int(config["database"]["port"])

    try:
        if DB_TYPE == "postgres":
            conn = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                port=DB_PORT
            )
            print("Connected to PostgreSQL!")
        
        elif DB_TYPE == "mysql":
            conn = mysql.connector.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                port=DB_PORT
            )
            print("Connected to MySQL!")
        
        else:
            raise ValueError("Unsupported database type in config.ini")

        return conn

    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None