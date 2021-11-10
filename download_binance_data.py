import pandas as pd
import math
import os.path
import time
from binance.client import Client
import datetime import
from dateutil import parser
import tqdm
import numpy as np

path = os.getcwd()
os.chdir('/Users/username/Desktop') # set a directory where you would like to store the data

binance_api_key = '[]' # set your api key
binance_api_secret = '[]' # set your api secret key
binsizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
batch_size = 750
binance_client = Client(api_key=binance_api_key, 
                        api_secret=binance_api_secret) # get access to Client

def minutes_of_new_data(symbol, kline_size, data, source):
    if len(data) > 0:
        old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance":
        old = datetime.strptime('1 Jan 2017', '%d %b %Y')
    if source == "binance":
        new = pd.to_datetime(binance_client.get_klines(symbol=symbol,
                                                       interval=kline_size)[-1][0], unit='ms')
    return old, new

def downloadAllBinance(symbol, kline_size, save=False):
    filename = '%s-%s-data.csv' % (symbol, kline_size)
    if os.path.isfile(filename):
        data_df = pd.read_csv(filename)
    else:
        data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(
        symbol, kline_size, data_df, source="binance")
    delta_min = (newest_point - oldest_point).total_seconds() / 60
    available_data = math.ceil(delta_min / binsizes[kline_size])
    if oldest_point == datetime.datetime.strptime('1 Jan 2017', '%d %b %Y'):
        print('Downloading all available %s data for %s.' %
              (kline_size, symbol))
    else:
        print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (
            delta_min, symbol, available_data, kline_size))
    klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime(
        "%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"))
    data = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close',
                                         'volume', 'close_time', 'quote_av', 'trades',
                                         'tb_base_av', 'tb_quote_av', 'ignore'])
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        data_df = data_df.append(temp_df)
    else:
        data_df = data
    data_df.set_index('timestamp', inplace=True)
    if save:
        data_df.to_csv(filename)
    # print('All caught up..!')
    return data_df

 
tickers, filenames  = find_symbol_filenames(directory_to_raw_data = '')
  
tickers = ["BTCUSDT"] # symbols to download
timeElapsed = [] # stores time elampsed for each item
tickers_missed = [] # stores the tickers that were not dowloaded for troubleshoting
numOfTickers = len(tickers) # total number of tickers to download
iterations = numOfTickers # iterations left
freq = "1m" # data frequency

for symbol in tickers:

    try:
        startTimer = time.time()
        downloadAllBinance(symbol, freq, save=True)
        endTimer = time.time()
        timeElapsedPoint = endTimer - startTimer
        iterations += -1
        timeElapsed.append(timeElapsedPoint)
        expetedTimeLeftMinutes = round(np.mean(np.array(timeElapsed)) * iterations / 60, 1)
        percentageUploaded = round((1 - iterations / numOfTickers) * 100, 1)
        print('All available data for ' + symbol + ' downloaded.')
        print(str(percentageUploaded) + "% " + "downloaded/updated. The expected time to"
              + " complete is equal to " + str(expetedTimeLeftMinutes)
              + " minutes.")
    except:
        tickers_missed.append(symbol)
        print(symbol + " " + "was not downloaded.")
