from readConfig import config
import mysql.connector as connector
from _pytest.mark import param

def getListData(sqlString):
    """ Connect to the MySQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the MySQL database...')
        print('Do SQL ', sqlString)
        conn = connector.connect(**params)
        cur = conn.cursor()
        cur.execute(sqlString)
 
        print(cur)
        # display the PostgreSQL database server version
        result = []
        for i in cur.fetchall():
            result.append(i[0])
       
        cur.close()
        return result;
    except (Exception, connector.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def getListObject(sqlString):
    """ Connect to the MySQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()
 
        # connect to the PostgreSQL server
        print('Connecting to the MySQL database...')
        print('Do SQL ', sqlString)
        conn = connector.connect(**params)
        cur = conn.cursor()
        cur.execute(sqlString)
 
        # display the PostgreSQL database server version
        result = cur.fetchall()       
        cur.close()
        return result;
    except (Exception, connector.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')