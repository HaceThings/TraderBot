import requests
import numpy as np
import json
import pandas as pd

#API Key for Alpha Vantage (Need to move to a separate file)
api_key = "QFAYI1XPUGD6UF9O"
symbol = "IBM"

# Gathers stock data from Alpha Vantage. 
def get_stock_data(symbol="T", interval="5min", type="TIME_SERIES_INTRADAY"): 
    """
    Gathers stock data from Alpha Vantage. 
    Inputs: symbol = stock ticker, interval = time interval of intra data, type
    """
    res = requests.get("https://www.alphavantage.co/query?function=" + type + "&symbol=" + symbol + "&interval=" + interval  + "&apikey=" + api_key)
    data = json.loads(res.text)
    #Insert dataframe here
    print(data)

def main():
    get_stock_data()

if __name__ == '__main__': 
    main()




