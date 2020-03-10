import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """Create and connect to sparkify database
    
    Args:
        N/A
    
    Output:
        It creates and connects to sparkify database
    """

    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """Used to drop tables
    
    Args:
        cur: cursor object for database connection
        conn: database connection details
    
    Output:
        Tables are dropped
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Used to create tables if it does not exist
    
    Args:
        cur: cursor object for database connection
        conn: database connection details
    
    Output:
        Tables are created
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function to run other python functions
    
    Args:
        N/A
    
    Output:
        Creates database and tables by executing other python functinos
    """    
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()