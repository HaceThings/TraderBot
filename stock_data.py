import requests
import numpy as np
import json
import pandas as pd

### DATA IMPORT ###
#Import all stock data from Alex
div_stocks = np.genfromtxt('C:/Users/jaceh/source/repos/HaceThings/TraderBot/StockData/Watchlist_DIV+Swings_2020_12_30.csv', delimiter=',', dtype=str)
pref_stocks = np.genfromtxt('StockData/Watchlist_Preferred+Stocks_2020_12_30.csv', delimiter=',', dtype=str)
risk_swing_stocks = np.genfromtxt('StockData/Watchlist_Risk+Swing_2020_12_30.csv', delimiter=',', dtype=str)

symbols = []

for rows in div_stocks:
    symbols.append(rows[0])


#API Key for Alpha Vantage (Need to move to a separate file)
api_key = "QFAYI1XPUGD6UF9O"
symbol = "IBM"

# Gathers stock data from Alpha Vantage. 
def get_stock_data(symbol="T", interval="5min", type="TIME_SERIES_INTRADAY"): 
    """
    Gathers stock data from Alpha Vantage. 
    Inputs: symbol = stock ticker
            interval = time interval of intra data
            type = 
    """
    res = requests.get("https://www.alphavantage.co/query?function=" + type + "&symbol=" + symbol + "&interval=" + interval  + "&apikey=" + api_key)
    data = json.loads(res.text)

    #Returns a group of the key-value pairs in the dictionary
    results = data.items()
    #Converts obj to a list
    temp_data = list(result)
    #Convert list to an array
    temp_array = np.array(temp_data)

    intraday_data = np.array(list(temp_array[1][1].items()))



def main():
    for sym in symbols:
        get_stock_data(sym)

if __name__ == '__main__': 
    main()




