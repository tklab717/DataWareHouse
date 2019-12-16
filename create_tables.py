import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries, drop_schema_queries


def drop_tables(cur, conn, schema):
    """
    The Function drops tables for staging and analysis in the database.
    
    Args:
    cur: Database cursor.
    conn: Connection for database
    schema: Schema for selected talbe

    Returns:
    None
    """
    for query in drop_table_queries:
        cur.execute(query.format(schema))
        conn.commit()

def drop_schemas(cur, conn, schema):
    """
    The Function drops schema in the database.
    
    Args:
    cur: Database cursor.
    conn: Connection for database
    schema: Schema for selected talbe

    Returns:
    None
    """
    
    for query in drop_schema_queries:
        cur.execute(query.format(schema))
        conn.commit()

def create_tables(cur, conn, schema):
    """
    The Function create tables for staging and analysis in the database.
    
    Args:
    cur: Database cursor.
    conn: Connection for database
    schema: Schema for selected talbe

    Returns:
    None
    """
    for query in create_table_queries:
        cur.execute(query.format(schema))
        conn.commit()
        
def create_schemas(cur, conn, schema):
    """
    The Function create schema in the database.
    
    Args:
    cur: Database cursor.
    conn: Connection for database
    schema: Schema for selected talbe

    Returns:
    None
    """
    for query in create_schema_queries:
        cur.execute(query.format(schema))
        conn.commit()
        
def main():
    """

    This is main function. Define connection of database and call
    the function for creating tables which are for staging and analysis in the database
    
    Args;
        None
    Return:
        None
    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    schema = 'pro_dwh'
    drop_tables(cur, conn, schema)
    drop_schemas(cur, conn, schema)
    create_schemas(cur, conn, schema)
    create_tables(cur, conn, schema)
    

    conn.close()


if __name__ == "__main__":
    main()