from Table_Data_Gateway import Table_Data_Gateway
from connection import Database_Connection

if __name__=="__main__":
    host_name = 'localhost'
    dbname = 'ourdb'
    port = '3306'
    username = 'root'
    password = 'rp@123'
    conn = None

    

    # Singleton Database Conection 
    DC = Database_Connection
    print(type(DC))
    conn = DC.connect_to_sql(host_name, port, username, password)
    curr = conn.cursor()
    
    # Create New Database
    TDG = Table_Data_Gateway
    DB_NAME = input('Enter The Database You Want to Use:---  ')
    TDG. create_database(curr,DB_NAME)

    
    # Create all Tables
    TDG.create_table(curr)

    # Add all Data from  JSON to Pandas to Database
    TDG.append_from_df_to_db(curr)

    conn.commit()
        
    # User Switch Case
    def user_input():
        user_in = int(input('''\n\nSelect One of the Following(SELECT,INSERT,UPDATE,DELETE Parameterized Query):
                    1.Show Tables (Account,branch,Customer,Loan)(Select)
                    2.Show Customer by ID(Select)
                    3.List of customers who have account in user given bank.(Select)
                    4.List of customers who have account balance greater than $50000 and loan amount paid is more than $60000. (Select)
                    5.Insert into Customer Table Values.(Insert)
                    6.Update into Branch Table Values.(Update)
                    7.Delete into Customer Table Values.(Delete)
                    8.List AccountID in whose Loan amount paid is less than $40,000 and Loan Duedate is before 2030-07-01(LeftJoin)
                    10.Exit or Quit From Program
                    User Input:-  
                    '''))

        if(user_in==1):
            table_name = input("\nEnter Table Name : Account , branch , Customer , Loan:--   ")
            TDG.show_table(curr,table_name)
            user_input()

        elif(user_in==2):
            cus_id = int(input("\nEnter Customer ID:-  "))
            TDG.customer_by_id(curr,cus_id)
            user_input()
        
        elif(user_in==3):
            c_in = input("\nEnter Branch Name: ")
            TDG.query_3(curr,c_in)
            user_input()
        
        elif(user_in==4):
            TDG.query_4(curr)
            user_input()
        
        elif(user_in==5):
            cust_id = int(input("\nEnter customer Id:-"))
            br_id = int(input("Enter Branch Id:- "))
            fr_name = input("Enter Firt Name:- ")
            ls_name = input("Enter Last Name:- ")
            dob = input('Enter Date of Birth in YYYY-MM-DD:- ')
            TDG.query_5(curr,cust_id, br_id, fr_name, ls_name, dob)
            conn.commit()
            user_input()
        
        elif(user_in==6):
            b_id = int(input("\nEnter the Branch ID which exists:- "))
            addr = input("Enter the New Address of Existing Bank:- ")
            TDG.query_6(curr,b_id,addr)
            conn.commit()
            user_input()
        
        elif(user_in==7):
            delete_id = int(input("\nEnter the Customer ID"))
            TDG.query_7(curr,delete_id)
            conn.commit()
            user_input()
        
        elif(user_in==8):
            TDG.query_8(curr)
            user_input()

        elif(user_in==10):
            print("Exit Successfully")
            conn.commit()
            conn.close()

        else:
            print("Enter Valid Input")
            user_input()


    #Call Switch Case Function
    user_input()

    