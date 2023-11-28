import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Load data from staging tables into the target tables using COPY statements.
    
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in copy_table_queries:
        # Execute COPY statement to load data from staging table to the target
        cur.execute(query)
        # Note that: You might want to uncomment the line below to commit changes
        # conn.commit()

def insert_tables(cur, conn):
    """
    Insert data from staging tables into the fact table and dimension tables using INSERT INTO SQL commands.
    
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in insert_table_queries:
        # Execute INSERT INTO statement to insert data into target tables
        cur.execute(query)
        # Commit changes to the database
        conn.commit()

def main():
    """
    Main ETL process that connects to the database, loads staging tables, and inserts data into the target tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Load data from staging tables
    load_staging_tables(cur, conn)

    # Insert data into target tables
    insert_tables(cur, conn)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()