import datetime
import psycopg2
import pandas as pd
import os
from datetime import date
from sqlalchemy import create_engine

daily_df = {}
daily_csv = os.listdir('./transform_data/daily_data')
hourly_df = {}
hourly_csv = os.listdir('./transform_data/hourly_data')

for file in daily_csv:
    daily_df[f'{file.split(".")[0]}'] = pd.read_csv(f'./transform_data/daily_data/{file}')

engine = create_engine('postgresql://postgres:password@localhost:5432/stocks')
for key, data in daily_df.items():
    print(data.head())
    data.to_sql(key, 
                con=engine,
                if_exists='replace',
                index=False,
                method='multi')
    
for file in hourly_csv:
    hourly_df[f'{file.split(".")[0]}'] = pd.read_csv(f'./transform_data/hourly_data/{file}')

engine = create_engine('postgresql://postgres:password@localhost:5432/stocks')
for key, data in hourly_df.items():
    print(data.head())
    data.to_sql(f'{key}_hourly', 
                con=engine,
                if_exists='replace',
                index=False,
                method='multi')    
# conn = psycopg2.connect(database='daily_data',
#                         user='postgres',
#                         password='password',
#                         host='127.0.0.1',
#                         port='5432')
# conn.autocommit = True

# cursor = conn.cursor() # create cursor object

# sql = ''''''
# cursor.execute(sql)

# conn.close()