import os
import psycopg
from dotenv import load_dotenv

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------

load_dotenv()

# --------------------------------------------------
# Create connection to Postgresql
# --------------------------------------------------

def get_connection():
    """
    Create and return a PostgreSQL connection using environment variables.
    """
    return psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

# --------------------------------------------------
# Execute query INSERT/UPDATE/DELETE
# --------------------------------------------------

def execute(query, params=None):
    """
    Execute a query that does not return rows (INSERT, UPDATE, DDL).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()

# --------------------------------------------------
# Execute query returning all rows
# --------------------------------------------------

def fetch_all(query, params=None):
    """
    Execute a query and return all rows.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()