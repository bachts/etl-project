### Pull data from spotify developer API
import pandas as pd
import numpy as np
import requests


#params for the call
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
    #print(symbol)
    meta_data = symbol['Meta Data']
    values = symbol[f'Time Series ({interval})']
    df = pd.DataFrame.from_dict(values)
    df.to_csv(f'./raw_data/hourly_data/{meta_data["2. Symbol"]}.csv')
# Meta data contains relevant info: Symbol