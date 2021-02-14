# Instruments for Trading Research

## Process Trading Data from Binance

Currently, a script to create Open, High, Low, Close (OHLC), Volume, Trades tables for several symbols per table was uploaded. The data was downloaded from `Binance` via `Binance API`. The script at a large extent uses `NumPy`. The tool is useful to process massive tables with long time-series of frequent data (seconds, minutes, etc.).

The script `download_binance_data.py` is a working version that allows downloading the trading data from `Binance` via `Binance API`. The `process_binance_data.py` script is created to aggregated data downloaded using `download_binance_data.py`.
