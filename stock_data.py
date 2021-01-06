import requests
import numpy as np
import json
import pandas as pd

### DATA IMPORT ###
#Import all stock data from Alex
div_stocks = np.genfromtxt('StockData/Watchlist_DIV+Swings_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1)
pref_stocks = np.genfromtxt('StockData/Watchlist_Preferred+Stocks_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1)
risk_swing_stocks = np.genfromtxt('StockData/Watchlist_Risk+Swing_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1)
stock_lists = [div_stocks, pref_stocks, risk_swing_stocks]

symbols = []

for x in range(0,len(stock_lists)):
    for rows in stock_lists[x]:
        temp = []
        temp.append(rows[0])
        if x == 0:
            temp.append("Dividend")
        elif x == 1:
            temp.append("Preferred")
        elif x == 2:
            temp.append("Risk Swing")
        else:
            temp.append("IDK")
        symbols.append(temp)

df = pd.DataFrame(symbols)

import sqlite3
conn = sqlite3.connect('intraday_data.db')
c = conn.cursor()
conn.commit()
df.to_sql('STOCK_MAIN', conn, if_exists='replace', index = False)



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
    convert_request_data(res, symbol)

def convert_request_data(res, symbol):
    time_stamps = []
    open = []
    high = []
    low = []
    close = []
    volume = []
    headers = ["Time Stamp", "Open", "High", "Low", "Close", "Volume"]
    
    conn = sqlite3.connect('intraday_data.db')
    c = conn.cursor()
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


        # TODO: Finish adding dynamic table adds for all symbols
        c.execute('CREATE TABLE ' + symbol.capitalize() + ' (TimeStamp datetime, Open decimal, High decimal, Low decimal, Close decimal, Volume int)')
        conn.commit()
        print(symbol.capitalize())
        df.to_sql(symbol.capitalize(), conn, if_exists='replace', index = False)
        conn.close()
    except Exception:
        pass
    

def main():
    for sym in symbols:
        get_stock_data(sym[0])

if __name__ == '__main__': 
    main()




