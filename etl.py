import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This query will be responsible for loading data from source data
    to our own for later transformation and load.

    Parameters:
           conn: connection to the database
           cur: cursor to work with database

    Returns:
          None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This query will be responsible for loading transformed data
    for analytical purpose.

    Parameters:
           conn: connection to the database
           cur: cursor to work with database

    Returns:
          None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()