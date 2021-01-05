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
    convert_request_data(res)

def convert_request_data(res):
    time_stamps = []
    open = []
    high = []
    low = []
    close = []
    volume = []
    headers = ["Time Stamp", "Open", "High", "Low", "Close", "Volume"]
    try:
        data = json.loads(res.text)
        ###Conversion from dictionary to pandas dataframe###
        #Returns a group of the key-value pairs in the dictionary
        result = data.items()
        #Converts obj to a list
        temp_data = list(result)
        #Convert list to an array
        temp_array = np.array(temp_data)
        intraday_data = np.array(list(temp_array[1][1].items()))
    
        for row in intraday_data:
            time_stamps.append(row[0])
            open.append(row[1]['1. open'])
            high.append(row[1]['2. high'])
            low.append(row[1]['3. low'])
            close.append(row[1]['4. close'])
            volume.append(row[1]['5. volume'])
    
        df = pd.DataFrame(time_stamps, columns=[headers[0]])
        df[[headers[1]]] = open
        df[[headers[2]]] = high
        df[[headers[3]]] = low
        df[[headers[4]]] = close
        df[[headers[5]]] = volume
        print(df)
    except Exception:
        pass
    

def main():
    for sym in symbols:
        get_stock_data(sym)

if __name__ == '__main__': 
    main()




