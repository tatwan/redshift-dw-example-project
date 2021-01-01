import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, drop_schemas, create_schemas


def drop_tables(cur, conn):
    """
    Drops all the existing tables.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('Tables Dropped')


def create_tables(cur, conn):
    """
    Creates all the necessary tables for ETL and DW.
    
    Output: Two staging tables created in the Staging_Area schema: 
            staging_events_table and staging_songss_table
            
            Five tables created in the Sparkify Schema: 1 Fact table and 4 Dimension Tables
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('Tables Created')
    
def initialize_schemas(cur, conn):
    """
    Drops and recreate the two schemas: Staging_Area schema for the staging tables, and Sparkify schema for the DW tables.
    """
    for query in drop_schemas:
        cur.execute(query)
        conn.commit()
    print('Schemas Dropped')
    for query in create_schemas:
        cur.execute(query)
        conn.commit()
    print('Staging_Area and Sparkify Schemas Created')

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    initialize_schemas(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()