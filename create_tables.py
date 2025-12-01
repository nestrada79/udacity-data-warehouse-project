import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import os
from dotenv import load_dotenv
load_dotenv()


def drop_tables(cur, conn):
    """
    Drops all tables listed in drop_table_queries.
    """
    for query in drop_table_queries:
        print(f"Dropping table with query:\n{query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables listed in create_table_queries.
    """
    for query in create_table_queries:
        print(f"Creating table with query:\n{query}")
        cur.execute(query)
        conn.commit()


def main():
    """
    - Loads configuration from dwh.cfg
    - Connects to Redshift using psycopg2
    - Drops all existing tables
    - Recreates them using SQL defined in sql_queries.py
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    host = os.getenv("REDSHIFT_HOST")
    dbname = config.get("CLUSTER", "DB_NAME")
    user = config.get("CLUSTER", "DB_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    port = int(config.get("CLUSTER", "DB_PORT"))


    print("Connecting to Redshift cluster...")
    conn_string = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    print("Connected successfully.")

    print("\nDropping existing tables (if any)...")
    drop_tables(cur, conn)

    print("\nCreating new tables...")
    create_tables(cur, conn)

    conn.close()
    print("\nAll tables created. Connection closed.")


if __name__ == "__main__":
    main()
