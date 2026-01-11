import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extras import execute_values
from psycopg2 import DatabaseError, InterfaceError

from dotenv import load_dotenv
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


# Load .env into environment variables
load_dotenv()  # looks for a .env file in the current working directory by default.[web:9]


def get_db_connection():
    """
    Return a new psycopg2 connection using environment variables.
    The function does NOT know anything about business logic / transformations.
    """
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        sslmode=os.getenv("PG_SSLMODE", "prefer"),
    )  # psycopg2.connect supports keyword arguments for connection parameters.[web:5][web:8]
    return conn


@contextmanager
def db_cursor(commit: bool = True, cursor_factory=DictCursor):
    """
    Context manager that yields a cursor and takes care of commit/rollback/close.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def bulk_insert_data_table(sql, data_table):
    conn = None
    try:
        # 1. Initialize connection and cursor
        conn = get_db_connection()
        cur = conn.cursor()

        # 2. Execute the batch insert
        execute_values(cur, sql, data_table, page_size=1000)

        # 3. Commit only if execution succeeds
        conn.commit()
        print(f"Successfully inserted {len(data_table)} rows into table")

    except (DatabaseError, InterfaceError) as db_err:
        # Rollback the transaction if any database error occurs
        if conn:
            conn.rollback()
        logging.error(f"Database error during insert into table: {db_err}")
        raise  # Re-raise so the orchestrator knows the pipeline failed

    except Exception as e:
        # Catch unexpected Python errors
        if conn:
            conn.rollback()
        logging.error(f"Unexpected error during transformation: {e}")
        raise

    finally:
        # 4. ALWAYS close the connection, regardless of success or failure
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")
