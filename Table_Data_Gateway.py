import pandas as pd
import json
import sys
import mysql.connector

class Table_Data_Gateway:

    #Read JSON File
    def json_file():
        try:
            fname = "data.json"
            f = open(fname)
        except FileNotFoundError:
            print(f"File {fname} not found.")
            sys.exit(1)
        except OSError:
            print(f"OS error occurred when trying to open {fname}")
            sys.exit(1)
        except Exception as err:
            print(f"Unexpected error opening {fname} is",repr(err))
            sys.exit(1)
        else:
            with f:
                data = json.load(f)
                return data


    #Create Database if Not Exist
    def create_database(curr,DB_NAME):
        try:
            curr.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
            print("Used Database Name:-- ", DB_NAME)
            curr.execute("USE {}".format(DB_NAME))
        except:
            curr.execute("USE {}".format(DB_NAME))
            print("Used Database :-- ",DB_NAME)



    #JSON to DataFrame Processing 
    def json_to_df(data):

        # Create A DataFrame From the JSON Data
        df_1 = pd.DataFrame(columns=["custid", "branch_id", "fname", "lname", "dob"], dtype=object, )
        for i in data:
            customerid = i['Customer_id']
            branchid = i['Branch_id']
            firstname = i['firstname']
            lastname = i['last_name']
            dateofbirth = i['DOB']
            

            df_1 = pd.concat([df_1, pd.DataFrame.from_records([{'custid':customerid,'branch_id':branchid,
                                "fname":firstname,"lname":lastname,
                                "dob":dateofbirth}])])


        df_2 = pd.DataFrame(columns=["branch_id", "branchname", "address"], dtype = object)
        for i in data:
            branchId = i['Branch_id']
            branchname = i['Name']
            address = i['Address']
            
            df_2 = pd.concat([df_2, pd.DataFrame.from_records([{'branch_id':branchId,'branchname':branchname,
                                "address":address}])])            

        df_2 = df_2.drop_duplicates()

        df_3 = pd.DataFrame(columns=["Accountid", "custid", "balance"], dtype=object)
        for i in data:
            accountid = i['Account_id']
            customerid = i['Customer_id']
            balance = i['balance']
            
            df_3 = pd.concat([df_3, pd.DataFrame.from_records([{'Accountid':accountid,'custid':customerid,
                                "balance":balance}])])


        df_4 = pd.DataFrame(columns=["loanid","accountid", "amtpaid", "startdate", "duedate"], dtype=object)
        for i in data:
            loanid = i['loan_id']
            accntid=i['Account_id']
            amountpaid = i['amount_paid']
            startdate = i['start_date']
            duedate = i['due_date']

            
            df_4 = pd.concat([df_4, pd.DataFrame.from_records([{'loanid':loanid,'accountid':accntid,
                                'amtpaid':amountpaid,'startdate':startdate,
                                'duedate':duedate}])])
    
        return df_2, df_1, df_3, df_4

    def append_from_df_to_db(curr):
        data = Table_Data_Gateway.json_file()
        df_2, df_1, df_3, df_4=Table_Data_Gateway.json_to_df(data)
        for i, row in df_2.iterrows():
                try:
                    branch_id = row['branch_id']
                    branchname = row['branchname']
                    address = row['address']

                    query = ("""INSERT INTO branch (branch_id, branchname, address)
                        VALUES(%s,%s,%s);""")
                    row_to_insert = (branch_id, branchname, address)

                    curr.execute(query, row_to_insert)
                except mysql.connector.IntegrityError as err:
                    pass
                    # print("Error: {}".format(err))
                    
        for i, row in df_1.iterrows():
                try:
                    customer_id = row['custid']
                    branchid = row['branch_id']
                    firstname = row['fname']
                    lastname = row['lname']
                    dateofbirth = row['dob']
                    

                    query = ("""INSERT INTO Customer (customer_id, branch_id, firstname, lastname, dateofbirth)
                            VALUES(%s,%s,%s,%s,%s);""")
                    row_to_insert = (customer_id, branchid, firstname, lastname, dateofbirth)

                    curr.execute(query, row_to_insert)
                except mysql.connector.IntegrityError as err:
                    pass
                    # print("Error: {}".format(err))
                    

        for i, row in df_3.iterrows():
                try:
                    accountid = row['Accountid']
                    customerid = row['custid']
                    balance = row['balance']
                    

                    query = ("""INSERT INTO Account (account_id, balance, customer_id)
                            VALUES(%s,%s,%s);""")
                    row_to_insert = (accountid, balance, customerid)

                    curr.execute(query, row_to_insert)
                except mysql.connector.IntegrityError as err:
                    pass
                    # print("Error: {}".format(err))
                    
        for i, row in df_4.iterrows():
                try:
                    loanid = row['loanid']
                    accountid = row['accountid']
                    amountpaid = row['amtpaid']
                    startdate = row['startdate']
                    duedate = row['duedate']
                    

                    query = ("""INSERT INTO Loan (loan_id, account_id, amountpaid, startdate, duedate)
                            VALUES(%s,%s,%s,%s,%s);""")
                    row_to_insert = (loanid, accountid, amountpaid, startdate, duedate)

                    curr.execute(query, row_to_insert)
                except mysql.connector.IntegrityError as err:
                    pass
                    # print("Error: {}".format(err))


    # Create Table Query
    def create_table(curr):
        create_table_branch = ("CREATE TABLE IF NOT EXISTS branch (branch_id INTEGER PRIMARY KEY, branchname TEXT, address TEXT)")

        create_table_customer = ("""CREATE TABLE IF NOT EXISTS Customer (customer_id INTEGER,
                                branch_id INTEGER,
                                firstname TEXT,
                                lastname TEXT,
                                dateofbirth DATE,
                                PRIMARY KEY (customer_id),
                                FOREIGN KEY(branch_id) REFERENCES branch (branch_id)) ENGINE=INNODB""")

        create_table_account= ("""CREATE TABLE IF NOT EXISTS Account (account_id INTEGER,
                                    balance INTEGER,
                                    customer_id INTEGER,
                                    PRIMARY KEY (account_id),
                                    FOREIGN KEY(customer_id) REFERENCES Customer (customer_id)) ENGINE=INNODB""")

        create_table_loan = ("""CREATE TABLE IF NOT EXISTS Loan (loan_id INTEGER,
                                        account_id INTEGER,
                                        amountpaid INTEGER,
                                        startdate DATE,
                                        duedate DATE,
                                        PRIMARY KEY (loan_id),
                                        FOREIGN KEY(account_id) REFERENCES Account (account_id)) ENGINE=INNODB""")
        curr.execute(create_table_branch)
        curr.execute(create_table_customer)
        curr.execute(create_table_account)
        curr.execute(create_table_loan)

        

    # Switch Case Queries 
    def show_table(curr,input):
        curr.execute("SELECT * FROM {}".format(input))
        for row in curr:
            print(row)

    def customer_by_id(curr,c_id):
        curr.execute("SELECT * FROM Customer WHERE customer_id={}".format(c_id))
        print("CustomerID--FirstName--LastName--BranchName")
        for row in curr:
            print(row)
    
    def query_3(curr,c_in):
        curr.execute('''SELECT customer.firstname, customer.lastname FROM customer, branch WHERE Customer.branch_id=branch.branch_id AND branch.branchname="{}"'''.format(c_in))
        print("FirstName---LastName")
        for row in curr:
            print(row)
    
    def query_4(curr):
        curr.execute("""SELECT customer.firstname, customer.lastname
                        FROM customer, account, loan
                        WHERE customer.customer_id = account.customer_id AND account.account_id = loan.account_id
                        AND account.balance>50000 AND loan.amountpaid>60000;""")
        print("\nFirstName----LastName")
        for row in curr:
            print(row)

    def query_5(curr, customer_id, branchid, firstname, lastname, dateofbirth):
        query = ("""INSERT INTO Customer (customer_id, branch_id, firstname, lastname, dateofbirth)
                            VALUES({},{},"{}","{}","{}")""".format(customer_id, branchid, firstname, lastname, dateofbirth))

        try:
            curr.execute(query)
            print("Inserted Successfully")
        except:
            print("Problem in Data")
    
    def query_6(curr,b_id,addr):
        query = ("""UPDATE Branch SET address="{}" WHERE branch_id ={}""".format(addr,b_id))

        try:
            curr.execute(query)
            print("Updated Successfully")
        except:
            print("Error:- Problem in Data")
    
    def query_7(curr,d_id):
        query = ("""DELETE FROM Customer WHERE customer_id={} """.format(d_id))

        try:
            curr.execute(query)
            print("Deleted Successfully")
        except:
            print("Error: Data Not Deleted")
    
    def query_8(curr):
        query = ("""Select a.account_id, a.balance, l.amountpaid , l.duedate
                    FROM account a LEFT JOIN  loan l
                    ON a.account_id = l.account_id WHERE l.amountpaid<40000 AND l.duedate<'2030-07-01';""")
        try:
            curr.execute(query)
            print("\nAccountID---Balance--AmountPaid--DueDate")
            for row in curr:
                print(row)
        except:
            print("Error: Problem in Query")
