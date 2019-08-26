import datetime
import psycopg2

import random
from config import config

 
def connect_test():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def connect_command(command: str, commit: bool = False):
    """ Connect to the PostgreSQL database server and execute a command """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        print('Executing command...', command)
        cur.execute(command)
        if commit:
            print('Commiting...')
            cur.execute('COMMIT')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
            

def connect_command_no_comment(command: str, commit: bool = False):
    """ Connect to the PostgreSQL database server and execute a command with no prints """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        cur.execute(command)
        if commit:
            cur.execute('COMMIT')
 
        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
 

def data_insert_test(rows: int, commit_rows: int):
    """ Connect to the PostgreSQL database and insert data in table test.data_ingest_test """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        for i in range(rows):
            random.seed(i)
            random_number = random.random()*1000000
            insert_command = "INSERT INTO test.data_ingest_test VALUES (" + str(random_number) + ", 'Hello world !!!')"
            # execute a statement
            cur.execute(insert_command)

            if not i % commit_rows or i == rows :
                cur.execute('COMMIT')
                print('Commit at ',i, ' rows...', datetime.datetime.now())
 
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def data_insert_test_many(rows: int, commit_rows: int):
    """ Connect to the PostgreSQL database and insert data in table test.data_ingest_test
        Using executemany() command """
    conn = None
    list_rows = []
    insert_command = "INSERT INTO test.data_ingest_test VALUES (%s)"
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
      
        # create a cursor
        cur = conn.cursor()
        
        for i in range(rows):
            random.seed(i)
            random_number = random.random()*1000000
            # Create list with rows to insert
            list_rows.append((str(random_number), "Hello world using executemany!!!"))

            if not i % commit_rows or i == rows :
                # execute a statement
                cur.executemany(insert_command, list_rows)
                cur.execute('COMMIT')
                print('Commit at ',i, ' rows...', datetime.datetime.now())
                list_rows.clear()
 
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
 
if __name__ == '__main__':
    # insert row by row
    #data_insert_test(10000, 500)    

    # insert gorupmany rows
    data_insert_test_many(10000, 500)

    