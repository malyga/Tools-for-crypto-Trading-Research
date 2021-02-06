import datetime
import pandas as pd
import glob
import numpy as np
import time

def generateDatesArray(start_date, end_date, step = "60"):

    '''

    Function generates a pandas array with datetime data of a fixed frequency.

    Input:

        start_date: string. Should be written as '%Y-%m-%d %H:%M:%S'

        end_date: string. Should be written as '%Y-%m-%d %H:%M:%S'

        step: string. Default value: "60". Frequency, in seconds.

    Output:

        datesArray: Pandas Array.

    '''

    step = datetime.timedelta(seconds = step)
    startDate = start_date
    endDate = end_date

    listOfDates = []

    while startDate < endDate:
        listOfDates.append(startDate.strftime('%Y-%m-%d %H:%M:%S'))
        startDate += step
    datesArray = pd.DataFrame(np.asanyarray(listOfDates, dtype='datetime64'))
    datesArray.rename(columns = {0: "Date"}, inplace = True)

    return(datesArray)

def find_relevant_file_names(tickers_to_process, directory_to_raw_data):

    '''

    Function assumes that in a directory, a user stores .csv files.
    Each file stores trading data for a single symbol downloaded from
    Binance. The function selects from all files stored in the directory only
    those ones, that a user would like to create a table with OCHL, volume, Trades
    data and returns a list with filenames.

    Input:

        tickers_to_process: list. Should contain symbols of interest. For example
            ["BTCUSDT", "DOTUSDT"]

        directory_to_raw_data: string. A directory, in which a user stores .csv
            files to process.
    Output:

        selected_file_names, list.

    '''

    path = directory_to_raw_data + "*" + ".csv"
    all_file_names = glob.glob(path)
    all_tickers = [file_name.split('-')[0].split('/')[-1] for file_name in all_file_names]
    tickers_to_process = tickers_to_process

    selected_file_names =[directory_to_raw_data
                             + ticker
                             + '-1m-data.csv'
                             for ticker in tickers_to_process]

    return(selected_file_names)

def create_OCHLVT_tables(start_date, end_date, step, tickers_to_process, directory_to_raw_data, export_directory):

    '''
    The function provides a relatively fast way to generate
    Open, High, Low, Close Volume, Trades (OHLCVT) tables for the data
    downloaded from Binance using binance package.
    Each generated table contains a single type of the OCHLVT data
    for selected symbols. The tables have dimension a number of periods
    times a number of tickers.
    Function returns 6 .csv files.

    Input:
        start_date: string. Should be written as '%Y-%m-%d %H:%M:%S'

        end_date: string. Should be written as '%Y-%m-%d %H:%M:%S'

        step: string. Default value: "60". Frequency, in seconds.

        tickers_to_process: list. Should contain symbols of interest.
         For example ["BTCUSDT", "DOTUSDT"]

        directory_to_raw_data: string. A directory, in which
         a user stores .csv files to process.

        export_directory: string. A directory, in which
         a user stores .csv output files to process.

    Output:
        Six OCHLVT .csv files that are stored in export_directory.

    '''

    directory_to_raw_data = directory_to_raw_data
    columnIndexes = [1, 2, 3, 4, 5, 8]
    columnNames = ['open', 'high', 'low', 'close', 'volume', 'trades']
    vectorDates = generateDatesArray(start_date = start_date,
                                     end_date = end_date, step = step)

    relevant_file_names = find_relevant_file_names(tickers_to_process = tickers_to_process,
                                                   directory_to_raw_data = directory_to_raw_data)
    tickers = tickers_to_process
    fileNames = relevant_file_names
    iterations = len(columnIndexes)
    timeElapsed = []
    coinFlag = tickers[0][3:7]
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
                          + coinFlag \
                          + '-1m-data' + '.csv'

        finalTableDataFrame.to_csv(export_directory + fileNameToWrite, index=False)
        endTimer = time.time()
        timeElapsedPoint = endTimer - startTimer
        iterations += -1
        timeElapsed.append(timeElapsedPoint)
        expetedTimeLeftMinutes = round(np.mean(np.array(timeElapsed))\
                                  * iterations / 60, 2)
        finalTableDataFrame = None
        indexForColumnNames += 1

        print(ochlvFlag + " data" + " has been generated. " + str(iterations)
              + " files to generate left. " + "Expected time to complete: "
              + str(expetedTimeLeftMinutes) + " minutes.")

        print('The name of the generated file is ' + fileNameToWrite + '.')
        print()
    return()

start_date_input = datetime.datetime(2018, 1, 1, 0, 00, 00)
end_date_input = datetime.datetime(2021, 2, 5, 0, 00, 00)
tickers_usdt = ['ADAUSDT', 'ATOMUSDT']
directory_to_raw_data = '/Users/username/Desktop/'
directory_export = '/Users/username/Desktop/'

create_OCHLVT_tables(start_date = start_date_input, end_date = end_date_input,
                     step = 60,
                     tickers_to_process = tickers_usdt,
                     directory_to_raw_data = directory_to_raw_data,
                     export_directory = directory_export)
