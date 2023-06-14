### Pull data from spotify developer API
import pandas as pd
import numpy as np
import requests


#params for the call
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
    #print(symbol)
    meta_data = symbol['Meta Data']
    values = symbol['Time Series (Daily)']
    df = pd.DataFrame.from_dict(values)
    df.to_csv(f'./raw_data/daily_data/{meta_data["2. Symbol"]}.csv')
# Meta data contains relevant info: Symbol