import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """"Copy song data and log data into staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data into final tables from staging tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # connect to redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load staging tables and insert data into final tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # close connection to redshift database
    conn.close()


if __name__ == "__main__":
    main()