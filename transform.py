import pandas as pd
import numpy as np
import os

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


