import datetime
from detatime import timedelta
import pandas as pd
import os.path
import glob
import numpy as np
import time

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

def find_symbol_filenames(directory_to_raw_data, 
                          tickers_to_process = None, 
                          base_ticker = "USDT")
    
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


    
def create_OCHLVT_tables(start_date, end_date, 
                         step, directory_to_raw_data, 
                         export_directory, 
                         tickers_to_process = None, 
                         base_ticker = "USDT"):

    directory_to_raw_data = directory_to_raw_data

    columnIndexes = [1, 2, 3, 4, 5, 8]

    columnNames = ['open', 'high', 'low', 'close', 'volume', 'trades']

    vectorDates = generate_dates_vector(start_date = start_date,
                                        end_date = end_date, step = step)
                                        
    tickers, relevant_file_names  = find_symbol_filenames(directory_to_raw_data, 
                                                          tickers_to_process, 
                                                          base_ticker)
    fileNames = relevant_file_names
    iterations = len(columnIndexes)
    timeElapsed = []
    coinFlag = base_ticker
    indexForColumnNames = 0

    for columnIndex in columnIndexes:

        startTimer = time.time()
        finalTable = None
        finalTable = np.empty(shape=(len(vectorDates), len(fileNames)),
                              dtype='float')
        i = 0

        for fileName in fileNames:
            dataFrame = pd.read_csv(fileName, usecols=[0, columnIndex])
            ochlvFlag = dataFrame.columns[1]
            targetColumn = np.asanyarray(dataFrame.iloc[:, 1], dtype='float')
            timeStampsVector = np.asanyarray(dataFrame['timestamp'],
                                             dtype='datetime64')
            foundTimeStamps, indexIntersectBasis, indexIntersectLocal = \
                np.intersect1d(vectorDates, timeStampsVector, return_indices=True)
            finalTable[indexIntersectBasis, i] = targetColumn[indexIntersectLocal]
            i += 1

        finalTable[finalTable == 0] = np.nan
        finalTableDataFrame = pd.DataFrame(finalTable)
        finalTableDataFrame = pd.concat([vectorDates,
                                         finalTableDataFrame], axis=1)
        finalTableDataFrame.columns = ['Date'] + tickers

        star_date_name = str(start_date.year) + "-" \
                         + str(start_date.month) + "-" \
                         + str(start_date.day)
        end_date_name = str(end_date.year) + "-" \
                        + str(end_date.month) + "-" \
                        + str(end_date.day)

        fileNameToWrite = star_date_name + '_' \
                          + end_date_name + '-' \
                          + ochlvFlag + '-' \
                          + coinFlag + '.csv'

        finalTableDataFrame.to_csv(export_directory + fileNameToWrite, index=False)
        endTimer = time.time()
        timeElapsedPoint = endTimer - startTimer
        iterations += -1
        timeElapsed.append(timeElapsedPoint)
        expetedTimeLeftMinutes = round(Average(timeElapsed) * iterations / 60, 2)
        finalTableDataFrame = None
        indexForColumnNames += 1

        print(ochlvFlag + " data" + " has been generated. " + str(iterations)
              + " files to generate left. " + "Expected time to complete: "
              + str(expetedTimeLeftMinutes) + " minutes.")

        print('The name of the generated file is ' + fileNameToWrite + '.')
        print()

    return()

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
