from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import requests
import psycopg2
from sqlalchemy import create_engine


dag_path = os.getcwd()

# import sentry_sdk
# sentry_sdk.init(
#     dsn="https://d5b990cf142746e1b0514fb7692d7552@o4505362879873024.ingest.sentry.io/4505362884788224",

#     # Set traces_sample_rate to 1.0 to capture 100%
#     # of transactions for performance monitoring.
#     # We recommend adjusting this value in production.
#     traces_sample_rate=1.0
# )

#print(1/0)
def init_folders():
    raw_folder = os.path.join(dag_path, 'raw_data')
    transform_folder = os.path.join(dag_path, 'transform_data')

    if not os.path.exists(raw_folder):
        os.makedirs(raw_folder)
    if not os.path.exists(transform_folder):
        os.makedirs(transform_folder)
    
    daily_path = os.path.join(raw_folder, 'daily_data')
    hourly_path = os.path.join(raw_folder, 'hourly_data')

    if not os.path.exists(daily_path):
        os.makedirs(daily_path)
    if not os.path.exists(hourly_path):
        os.makedirs(hourly_path)

    daily_path = os.path.join(transform_folder, 'daily_data')
    hourly_path = os.path.join(transform_folder, 'hourly_data')

    if not os.path.exists(daily_path):
        os.makedirs(daily_path)
    if not os.path.exists(hourly_path):
        os.makedirs(hourly_path)
def extract_daily():
    key = 'ZZYZO7P5AHO0M0N4'
    function = 'TIME_SERIES_DAILY_ADJUSTED' #type of timeseries
    symbols = ['MSFT', 'GOOG', 'AMZN', 'AAPL', 'META'] #stock symbol
    interval = '15min' #interval between two data points
    raw_data = []
    for symbol in symbols:
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&outputsize=full&apikey={key}&datatype=json'
        r = requests.get(url)
        raw_data.append(r.json())
    for symbol in raw_data:
        meta_data = symbol['Meta Data']
        values = symbol['Time Series (Daily)']
        df = pd.DataFrame.from_dict(values)
        df.to_csv(f'{dag_path}/raw_data/daily_data/{meta_data["2. Symbol"]}.csv')
def extract_hourly():
    key = 'IF95HIZNQ2N5GDFJ'
    function = 'TIME_SERIES_INTRADAY' #type of timeseries
    symbols = ['MSFT', 'GOOG', 'AMZN', 'AAPL', 'META'] #stock symbol
    interval = '60min' #interval between two data points

    raw_data = []

    for symbol in symbols:
        url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey={key}&datatype=json'
        r = requests.get(url)
        raw_data.append(r.json())

    for symbol in raw_data:
        meta_data = symbol['Meta Data']
        values = symbol[f'Time Series ({interval})']
        df = pd.DataFrame.from_dict(values)
        df.to_csv(f'{dag_path}/raw_data/hourly_data/{meta_data["2. Symbol"]}.csv')
def transform():
    def moving_average(df): #moving average of a stock
        close = df['adjusted close']
        mavg = close.rolling(window=100).mean()
        return mavg
    def return_deviation(df): #expected return of a stock
        close = df['adjusted close']
        rets = close / close.shift(1) - 1
        return rets



#data derivation

    daily_df = {}
    hourly_df = {}

    daily_csv = os.listdir(f'{dag_path}/raw_data/daily_data')
    hourly_csv = os.listdir(f'{dag_path}/raw_data/hourly_data')

    for file in daily_csv:
        daily_df[f'{file.split(".")[0]}'] = pd.read_csv(f'{dag_path}/raw_data/daily_data/{file}')
    for file in hourly_csv:
        hourly_df[f'{file.split(".")[0]}'] = pd.read_csv(f'{dag_path}/raw_data/hourly_data/{file}')


#Modify the dataframes

    for key, df in daily_df.items():
        df = df.rename(columns={'Unnamed: 0': 'Indicator'})
        df['Indicator'] = df['Indicator'].str.split('.').str[1].str.lstrip(' ') # Remove 1. from start of the indicators
        df = df.set_index('Indicator')
        df = df.transpose()
        df = df.reset_index()
        df = df.sort_values('index')
        daily_df[key] = df
    for key, df in hourly_df.items():
        df = df.rename(columns={'Unnamed: 0': 'Indicator'})
        df['Indicator'] = df['Indicator'].str.split('.').str[1].str.lstrip(' ') # Remove 1. from start of the indicators
        df = df.set_index('Indicator')
        df = df.transpose()
        df = df.reset_index()
        df = df.sort_values('index')
        df = df.rename(columns={'index': 'datetime'})
        df = df.fillna('None')
        hourly_df[key] = df
    for key, df in daily_df.items():
        mavg = moving_average(df)
        ret_dev = return_deviation(df)
        df['mavg'] = mavg
        df['ret_dev'] = ret_dev
        df = df.reset_index(drop=True)
        df = df.rename(columns={'index': 'datetime'})
        df = df.fillna('None')
        daily_df[key] = df

# save the dataframes
    for key, df in daily_df.items():
        df.to_csv(f'{dag_path}/transform_data/daily_data/{key}.csv', index=False)
    for key, df in hourly_df.items():
        df.to_csv(f'{dag_path}/transform_data/hourly_data/{key}.csv', index=False)
def load():
    daily_df = {}
    daily_csv = os.listdir(f'{dag_path}/transform_data/daily_data')
    hourly_df = {}
    hourly_csv = os.listdir(f'{dag_path}/transform_data/hourly_data')

    for file in daily_csv:
        daily_df[f'{file.split(".")[0]}'] = pd.read_csv(f'{dag_path}/transform_data/daily_data/{file}')

    engine = create_engine('postgresql://postgres:password@localhost:5432/stocks')
    for key, data in daily_df.items():
        print(data.head())
        data.to_sql(key, 
                con=engine,
                if_exists='replace',
                index=False,
                method='multi')
    
    for file in hourly_csv:
        hourly_df[f'{file.split(".")[0]}'] = pd.read_csv(f'{dag_path}/transform_data/hourly_data/{file}')

    engine = create_engine('postgresql://postgres:password@localhost:5432/stocks')
    for key, data in hourly_df.items():
        print(data.head())
        data.to_sql(f'{key}_hourly', 
                con=engine,
                if_exists='replace',
                index=False,
                method='multi')   

default_args = {
    'owner': 'bach',
    #'retry_delay': timedelta(minute=1),
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'stocks_dag',
    default_args=default_args,
    description='Extracting stock data for analysis and visualization',
    schedule_interval=timedelta(minutes=5),
    catchup=False
)
init = PythonOperator(
    task_id = 'initialize folders',
    python_callable=init_folders,
    dag=ingestion_dag
)

task_1 = PythonOperator(
    task_id='daily_data',
    python_callable=extract_daily,
    dag=ingestion_dag
)

task_2 = PythonOperator(
    task_id='hourly_data',
    python_callable=extract_hourly,
    dag=ingestion_dag
)

task_3 = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=ingestion_dag
)

task_4 = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=ingestion_dag
)

init >> [task_1, task_2] >> task_3 >> task_4
