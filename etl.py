import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes all EL scripts to extract & Load multiple JSON files from S3 bucket and load into Staging Tables.
    
    Output: The two staging tables in Staging_Area schema are loaded with the data from the source JSON files in S3. 
    """
    print('Staging Tables Loading Started ...')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print('Loading Complete')


def insert_tables(cur, conn):
    """
    Executes all the ETL (Extract, Transform, and Load) data from Staging_Area schema into the Sparkify schema.
    
    Output: All fact and dimension tables are loaded with the data from the Staging Area.
    """
    print('DW Tables Loading Started ...')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print('DW Loading Complete')


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