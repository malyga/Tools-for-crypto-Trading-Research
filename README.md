# Instruments for Trading Research

Although there are many similar resources on the web that help to address trading research challanges, I find that some of them are either do not exist, or not reliable enough (requires time to thorough testing) to use in a day-to-day research.<br>
Thus, I would like to propose several functions, and, at the end, a package that provides a range of robust and useful tools for trading and investment research.
The project is in its very beginning stage. 

## Process Trading Data from Binance
Currently, only a relatively fast script to create Open, High, Low, Close (OHLC), Volume, Trades tables for several symbols per table was uploaded. The data was downloaded from `Binance` via `Binance API`. The script at a large extent uses `NumPy` computational power and useful processing frequent data (ticks, minutes)(more detailed explanantion will be added).<br>
