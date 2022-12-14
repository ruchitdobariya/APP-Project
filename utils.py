import mysql.connector
from mysql.connector import errorcode

config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'rp@123',
    'database': 'abc'
}

def db_read(query, params=None):
    try:
        conn = mysql.connector.connect(**config)
        curr = conn.cursor(dictionary=True)
        if params:
            curr.execute(query, params)
        else:
            curr.execute(query)

        get_rows = curr.fetchall()
        curr.close()
        conn.close()

        content = []

        for x in get_rows:
            content.append(x)

        return content

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("User authorization error")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database doesn't exist")
        else:
            print(err)
    else:
        conn.close()
    finally:
        if conn.is_connected():
            curr.close()
            conn.close()
            print("Connection closed")
       
def db_write(query, params=None):
    try:
        conn = mysql.connector.connect(**config)
        curr = conn.cursor(dictionary=True)
        try:
            curr.execute(query, params)
            conn.commit()
            curr.close()
            conn.close()
            return True

        except:
            curr.close()
            conn.close()
            return False

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("User authorization error")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database doesn't exist")
        else:
            print(err)
        return False
    else:
        conn.close()
        return False
    finally:
        if conn.is_connected():
            curr.close()
            conn.close()
            print("Connection closed")