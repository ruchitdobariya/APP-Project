import requests
import json
import time
import mysql.connector
import pandas as pd
import psycopg2 as ps

offset = 0
limit = 10

responses = []
total = 512064

while(True):
  url = f"https://wft-geo-db.p.rapidapi.com/v1/geo/cities?offset={offset}&limit={limit}"

  headers = {
    "X-RapidAPI-Key": "91e651e76dmsh4c3a464350dde1dp1b76c3jsn72c8aab6a64c",
    "X-RapidAPI-Host": "wft-geo-db.p.rapidapi.com"
  }

  responses.append(requests.request("GET", url, headers=headers).json())

  if(offset+limit>=1000):
    break;
  time.sleep(1.5)
  offset += limit

# print(len(responses))
responses[0]['data']

df1 = pd.DataFrame(columns=["city_id","city_name","longitude", "latitude"])

for j in range(0, len(responses)):
  for i in responses[j]['data']:
    city_id = i['id']
    city_name = i['name']
    longitude = i['longitude']
    latitude = i['latitude']

    df1 = df1.append({"city_id":city_id, "city_name":city_name, "longitude":longitude, "latitude":latitude}, ignore_index = True)

mycursor.execute("CREATE DATABASE ourdb")

def connect_to_db(host_name, dbname, port, username, password):
    try:
        conn = mysql.connector.connect(host=host_name, database=dbname, user=username, passwd=password, port=port)

    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
        return conn


def create_table(curr):
    create_table_command = ("CREATE TABLE IF NOT EXISTS City (city_id INTEGER PRIMARY KEY, city_name TEXT NOT NULL, longitude FLOAT NOT NULL, latitude FLOAT NOT NULL)")

    curr.execute(create_table_command)


def insert_into_table(curr, city_id, city_name, longitude, latitude):
    insert_into_videos = ("""INSERT INTO City (city_id, city_name, longitude, latitude)
    VALUES(%s,%s,%s,%s);""")
    row_to_insert = (city_id, city_name, longitude, latitude)
    curr.execute(insert_into_videos, row_to_insert)


def update_row(curr, city_id, city_name, longitude, latitude):
    query = ("UPDATE City SET city_name = %s, longitude = %s, latitude = %s WHERE city_id = %s")
    vars_to_update = (city_name, longitude, latitude, city_id)
    curr.execute(query, vars_to_update)


def check_if_city_exists(curr, city_id):
    query = (f"SELECT city_id FROM City WHERE city_id = {city_id}")
    print(query)
    curr.execute(query)
    return curr.fetchone() is not None


def append_from_df_to_db(curr,df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['city_id'], row['city_name'], row['longitude'], row['latitude'])


def update_db(curr, df):
    tmp_df = pd.DataFrame(columns=['city_id', 'city_name', 'longitude', 'latitude'])
    for i, row in df.iterrows():

        print(row['city_id'])
        if check_if_city_exists(curr, row['city_id']):  # If city already exists then we will update
            update_row(curr, row['city_id'], row['city_name'], row['longitude'], row['latitude'])
            print("case 1")
        else:  # The city doesn't exists so we will add it to a temp df and append it using append_from_df_to_db
            tmp_df = tmp_df.append(row)
            print("case 2")
    return tmp_df


host_name = 'localhost'
dbname = 'ourdb'
port = '3306'
username = 'root'
password = 'Ruchit@1905'
conn = None

conn = connect_to_db(host_name, dbname, port, username, password)
curr = conn.cursor()

create_table(curr)

new_city_df3 = update_db(curr,df1)
conn.commit()

append_from_df_to_db(curr, new_city_df3)
conn.commit()