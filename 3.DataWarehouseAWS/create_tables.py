import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries

logging.basicConfig(level=logging.INFO)

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    logging.info("Connecting to Redshift cluster...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logging.info("Connected")

    logging.info("Dropping tables before start...")
    drop_tables(cur, conn)
    logging.info("Creating new tables...")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()