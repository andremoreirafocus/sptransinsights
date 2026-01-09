import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

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
