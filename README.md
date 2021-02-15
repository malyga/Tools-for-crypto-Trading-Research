
# Download and Process Trading Data from Binance


The script `download_binance_data.py` is a working version that allows to download the trading data from `Binance` via `Binance API`.

`process_binance_data.py` is created to generate Open, High, Low, Close (OHLC), Volume tables separately for a number of symbols per one table.
It is assumed that the input data for this script downloaded from `Binance` via `Binance API` using `download_binance_data.py`. The script at a large extent uses `NumPy`. The tool is useful to process massive tables with long time-series of frequent data (seconds, minutes, etc.).


