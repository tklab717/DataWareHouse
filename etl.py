import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, schema):
    """
    The Function load data from S3 to staging tables which user made.
    
    Args:
    cur: Database cursor.
    conn: Connection for database
    schema: Schema for selected talbe

    Returns:
    None
    """
    
    for query in copy_table_queries:
        cur.execute(query.format(schema))
        conn.commit()


def insert_tables(cur, conn, schema):
    """
    The Function insert data which include staging tables to each analytical table which user made.
    
    Args:
    cur: Database cursor.
    conn: Connection for database
    schema: Schema for selected talbe

    Returns:
    None
    """
    for query in insert_table_queries:
        cur.execute(query.format(schema))
        conn.commit()

def main():
    """

    This is main function. Define connection of database and call
    the function for loading staging data from S3 and inserting the data to analytical tables 
    
    Args;
        None
    Return:
        None
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get('CLUSTER','HOST'), config.get('CLUSTER','DB_NAME'), config.get('CLUSTER','DB_USER'), config.get('CLUSTER','DB_PASSWORD'), config.get('CLUSTER','DB_PORT')))
    cur = conn.cursor()
    schema = 'pro_dwh'
    load_staging_tables(cur, conn, schema)
    insert_tables(cur, conn, schema)

    conn.close()

if __name__ == "__main__":
    main()