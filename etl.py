import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from dotenv import load_dotenv
import os


def load_staging_tables(cur, conn):
    """
    Executes COPY commands to load data from S3 into staging tables.
    """
    print("\n=== Loading data into staging tables ===\n")
    for query in copy_table_queries:
        print("Executing COPY command:\n", query)
        cur.execute(query)
        conn.commit()
        print("COPY completed.\n")


def insert_tables(cur, conn):
    """
    Executes INSERT statements to transform and load data
    into the star schema analytics tables.
    """
    print("\n=== Inserting data into analytics tables ===\n")
    for query in insert_table_queries:
        print("Executing INSERT:\n", query)
        cur.execute(query)
        conn.commit()
        print("INSERT completed.\n")


def main():
    """
    - Loads .env variables
    - Reads configuration from dwh.cfg
    - Connects to Redshift
    - Loads staging tables
    - Loads analytics tables
    """
    load_dotenv()  # Load AWS keys and Redshift credentials

    # Load configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Pull values from .env and dwh.cfg
    host = os.getenv("REDSHIFT_HOST")
    dbname = config.get("CLUSTER", "DB_NAME")
    user = config.get("CLUSTER", "DB_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    port = int(config.get("CLUSTER", "DB_PORT"))

    # Connect to Redshift
    print("Connecting to Redshift...")
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    print("Connection established.\n")

    # Run ETL steps
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    print("\n=== ETL completed successfully. Connection closed. ===")


if __name__ == "__main__":
    main()
