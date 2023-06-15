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
        df.to_csv(f'./raw_data/daily_data/{meta_data["2. Symbol"]}.csv')
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
        df.to_csv(f'./raw_data/hourly_data/{meta_data["2. Symbol"]}.csv')
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

    daily_csv = os.listdir('./raw_data/daily_data')
    hourly_csv = os.listdir('./raw_data/hourly_data')

    for file in daily_csv:
        daily_df[f'{file.split(".")[0]}'] = pd.read_csv(f'./raw_data/daily_data/{file}')
    for file in hourly_csv:
        hourly_df[f'{file.split(".")[0]}'] = pd.read_csv(f'./raw_data/hourly_data/{file}')


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
        df.to_csv(f'./transform_data/daily_data/{key}.csv', index=False)
    for key, df in hourly_df.items():
        df.to_csv(f'./transform_data/hourly_data/{key}.csv', index=False)
def load():
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

[task_1, task_2] >> task_3 >> task_4
