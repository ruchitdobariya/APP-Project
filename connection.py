import mysql.connector
from mysql.connector import errorcode

class Database_Connection:
    
    #Database Connection Method
    def connect_to_sql(host_name, port, username, password):    
        
        #Handle Connection Errors
        try:
            conn = mysql.connector.connect(host=host_name, user=username, passwd=password, port=port)

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
                raise err
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
                raise err
            else:
                print(err)
        else:
            print("Connected to SQL!")
            return conn
    
    
