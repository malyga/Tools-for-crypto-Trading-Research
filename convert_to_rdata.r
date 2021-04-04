##### USDT #####
close <- read.csv(file = "close.csv")
high <- read.csv(file = "high.csv")
low <- read.csv(file = "low.csv")
volume <- read.csv(file = "volume.csv")
trades <- read.csv(file = "trades.csv")

rownames(close) <- close[,1]
close <- close[,-1]
close <- as.xts(close)

rownames(high) <- high[,1]
high <- high[,-1]
high <- as.xts(high)

rownames(low) <- low[,1]
low <- low[,-1]
low <- as.xts(low)

rownames(volume) <- volume[,1]
volume <- volume[,-1]
volume <- as.xts(volume)

Data <- new.env()
Data$closes <- close
Data$highs <- high
Data$lows <- low
Data$volumes <- volume

save(Data, file = "Data.RData")
##### end #####

##### BTC #####
close <- read.csv(file = "close.csv")
high <- read.csv(file = "high.csv")
low <- read.csv(file = "low.csv")
volume <- read.csv(file = "volume.csv")

rownames(close) <- close[,1]
close <- close[,-1]
close <- as.xts(close)

rownames(high) <- high[,1]
high <- high[,-1]
high <- as.xts(high)

rownames(low) <- low[,1]
low <- low[,-1]
low <- as.xts(low)

rownames(volume) <- volume[,1]
volume <- volume[,-1]
volume <- as.xts(volume)

Data <- new.env()
Data$closes <- close
Data$highs <- high
Data$lows <- low
Data$volumes <- volume

save(Data, file = "Data.RData")
##### end #####
