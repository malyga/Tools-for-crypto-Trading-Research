# Instruments For (Crypto) Trading Research
The project aims to provide a user with a variety of instruments to make a trading research (primarily within a crypto space) easier.
Currently, the project in a beginning stage.

This commit contains a data processing script that provides a relatively fast way to create tables with Open, Close, High, Low (OCHL) prices, Volume and Trade data out of the trading data downloaded from `Binance`. Each table stores a single type of data (e.g., close prices), but for a number of symbols/tickers.

While performing a trading research, in general, it is useful to analyze various statistical relationships over a number of instruments/securities selecting only a single data type.
