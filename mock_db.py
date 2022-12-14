from unittest import TestCase
import mysql.connector
from mysql.connector import errorcode
from mock import patch
import utils


MYSQL_USER = "root"
MYSQL_PASSWORD = "rp@123"
MYSQL_DB = "testdb"
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"


class MockDB(TestCase):

    @classmethod
    def setUpClass(cls):
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port = MYSQL_PORT
        )
        curr = conn.cursor(dictionary=True)

        try:
            curr.execute("DROP DATABASE {}".format(MYSQL_DB))
            curr.close()
            print("DB dropped")
        except mysql.connector.Error as err:
            print("{}{}".format(MYSQL_DB, err))

        curr = conn.cursor(dictionary=True)
        try:
            curr.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(MYSQL_DB))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)
        conn.database = MYSQL_DB

        query = """CREATE TABLE IF NOT EXISTS branch (branch_id INTEGER PRIMARY KEY, branchname TEXT, address TEXT)"""

        try:
            curr.execute(query)
            conn.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("Test_Branch_table already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

        insert_query = """INSERT INTO branch (branch_id, branchname, address) VALUES
                            ('1', 'branch1', 'abc'),
                            ('2', 'branch2','def')"""
        try:
            curr.execute(insert_query)
            conn.commit()
        except mysql.connector.Error as err:
            print("Data insert failed \n" + err)
        curr.close()
        conn.close()

        testconfig ={
            'host': MYSQL_HOST,
            'user': MYSQL_USER,
            'password': MYSQL_PASSWORD,
            'database': MYSQL_DB
        }
        cls.mock_db_config = patch.dict(utils.config, testconfig)

    @classmethod
    def tearDownClass(cls):
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        curr = conn.cursor(dictionary=True)

        # drop test database
        try:
            curr.execute("DROP DATABASE {}".format(MYSQL_DB))
            conn.commit()
            curr.close()
        except mysql.connector.Error as err:
            print("Database {} does not exists. Dropping db failed".format(MYSQL_DB))
        conn.close()
