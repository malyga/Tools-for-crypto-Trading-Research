# Download and Process Trading Data from Binance

## Information about the scripts:

* The `download_and_process.py` script is a working version that allows to download the trading data (OHLC, Volume, Trades, etc.) from `Binance` via `Binance API`.
The script downloads the data per a symbol and stores it within a directory as .csv files. Besides, the script is created to generate OHLC, Volume tables separately. Each table contains one type of data (e.g., close prices) for several tickers. This format is handy while designing trading strategies. The tool is useful to process massive datasets with time-series of frequent data (seconds, minutes, etc.). Currently, the script are limited in terms of documentation and comments.
