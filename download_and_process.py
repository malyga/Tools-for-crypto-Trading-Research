import pandas as pd
import math
import os.path
import glob
import time
from binance.client import Client
import datetime import
from detatime import timedelta
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

def find_symbol_filenames(directory_to_raw_data, 
                          tickers_to_process = None, 
                          base_ticker = "USDT"):
    
    """
    The function is created to find necessary price data among a variety
    of cryptopairs in `directory_to_raw_data`. It is possible to specify
    cryptopairs via `tickers_to_process` or define `base_ticker` which is used
    to find all cryptopairs that are traded to `base_ticker` (e.g., given
    `base_ticker` = "USDT", the "BTCUSDT" pair will be found). 
    The function requires that filenames that are stored in `directory_to_raw_data`
    to follow the following name pattern: "Ticker.csv". For instance,
    "BTCUSDT.csv".
    
    Arguments:
        directory_to_raw_data -- string, path to directory where price data is stored
        tickers_to_process -- list, the data for the tickers in the list will be searched (e.g., ["BTCUSDT", "XRPBTC"])
        base_ticker -- string, all pairs associated with this ticker will be searched (e.g., ["BTCUSDT", "ETHUSDT"])
        base_ticker = "USDT"
    Returns:
        tickers_to_process -- list of tickers that were searched (auxiliary)
        selected_file_names -- list of filenames found
    """
    
    path = directory_to_raw_data + "*" + ".csv"
    all_file_names = glob.glob(path)
    all_tickers = [file_name.split('-')[0].split('/')[-1] for file_name in all_file_names]
    usdt_tickers = []
    btc_tickers = []
    for i in range(0, len(all_tickers)):
        try:
            if(isinstance(all_tickers[i].index("USDT"), int) == True):
                usdt_tickers.append(all_tickers[i])
        except:
            if(isinstance(all_tickers[i].index("BTC"), int) == True):
                btc_tickers.append(all_tickers[i])
    if (tickers_to_process == None) & (base_ticker == "USDT"):
        tickers_to_process = usdt_tickers
        
    elif (tickers_to_process == None) & (base_ticker == "BTC"):
        tickers_to_process = btc_tickers
    
    selected_file_names =[directory_to_raw_data
                             + ticker
                             + '.csv'
                             for ticker in tickers_to_process]
    return(tickers_to_process, selected_file_names)
 

        
def Average(lst):

    return sum(lst) / len(lst)

def generate_dates_vector(start_date, end_date, step = 60):
    
    """
    The function generates a sequence of dates with a fixed step (seconds).
    
    Dependencies: datetime and pandas packages
     Arguments:
        start_date -- datetime object. Example: datetime.datetime(2018, 1, 1, 0, 00, 00) # '%Y-%m-%d %H:%M:%S'       
        end_date -- datetime object. Example: datetime.datetime(2021, 1, 1, 0, 00, 00) # '%Y-%m-%d %H:%M:%S'    
    Returns:
        vectorDates -- pandas data frame with 1 column and number of rows equal to the number of periods.
    """
    
    step = timedelta(seconds = step)
    startDate = start_date
    endDate = end_date

    vectorDates = []

    while startDate < endDate:
        vectorDates.append(startDate.strftime('%Y-%m-%d %H:%M:%S'))
        startDate += step
    vectorDates = pd.DataFrame(np.asanyarray(vectorDates, dtype='datetime64'))
    vectorDates.rename(columns = {0: "Date"}, inplace = True)
    return(vectorDates)
 
tickers, filenames  = find_symbol_filenames(directory_to_raw_data = '')
  
tickers = ["BTCUSDT"] # symbols to download
timeElapsed = [] # stores time elampsed for each item
tickers_missed = [] # stores the tickers that were not dowloaded for troubleshoting
numOfTickers = len(tickers) # total number of tickers to download
iterations = numOfTickers # iterations left
freq = "1m" # data frequency {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}

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

start_date_input = datetime.datetime(2018, 1, 1, 0, 00, 00) # '%Y-%m-%d %H:%M:%S'
end_date_input = datetime.datetime(2021, 2, 15, 12, 00, 00) # '%Y-%m-%d %H:%M:%S'
directory_to_raw_data = ''
directory_export = ''
path = directory_to_raw_data + "*" + ".csv"
all_file_names = glob.glob(path)


create_OCHLVT_tables(start_date = start_date_input, 
                     end_date = end_date_input, 
                     step = 60,
                     directory_to_raw_data = directory_to_raw_data,
                     export_directory = directory_export,
                     tickers_to_process = None,
                     base_ticker = "USDT")
