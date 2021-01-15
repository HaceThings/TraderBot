import requests
import numpy as np
import json
import pandas as pd
import sqlite3
import time
from bs4 import BeautifulSoup

class data_collector():
    def __init__(self):
        self.symbol_list = []
        self.db_filename = 'demo.db'
        self.api_key = "QFAYI1XPUGD6UF9O"

    def create_db(self):
        try:
            conn = sqlite3.connect(self.db_filename)
            c = conn.cursor()
            # Create a table to store stock information
            c.execute('CREATE TABLE STOCK_INFO (Symbol text, Sector text, DividendYield decimal, DividendDate datetime, ExDividendDate datetime)')
            # Create a table to store stock intraday data
            c.execute('CREATE TABLE STOCK_DATA (TimeStamp datetime, Open decimal, High decimal, Low decimal, Close decimal, Volume int)')
            conn.commit()
            print(sqlite3.version)
        except Exception as e:
            print(e)
        finally:
            if conn:
                conn.close()

    def get_db_filename(self):
        return self.db_filename

    def set_db_filename(self, db_filename):
        self.db_filename = db_filename

    def get_symbol_list(self):
        return self.symbol_list

    def set_symbol_list(self, symbol_list):
        self.symbol_list = symbol_list

    def get_stock_information_single(self, symbol):
         res = requests.get("https://www.alphavantage.co/query?function=" + "OVERVIEW" + "&symbol=" + symbol + "&apikey=" + self.api_key)
         res_data = json.loads(res.text)
         
         try:
            conn = sqlite3.connect(self.db_filename)
            c = conn.cursor()
            data = [res_data['Symbol'], res_data['DividendYield'], res_data['DividendDate'], res_data['ExDividendDate'], res_data['DividendPerShare']]
            c.execute('INSERT INTO STOCK_INFO (Symbol,Sector,DividendYield,DividendDate,ExDividendDate) VALUES (?, ?, ?, ?, ?)', data)
            conn.commit()
            conn.close()
            print(data)

         except Exception as e:
            print(e)
            pass

    def get_stock_information_list(self):
        for symbol in self.get_symbol_list():
            self.get_stock_information_single(symbol)
            time.sleep(12)

    def get_dividend_history(self, symbol):
        """Gets dividend history for a given stock."""
        ex_dividend = []
        payoutDate = []
        cash_amount = []
        percent_change = []
        symbols = []

        res = requests.get("https://dividendhistory.org/payout/"+ symbol + "/")
        soup = BeautifulSoup(res.text)

        dividend_table = soup.find_all("tr")
        for row in dividend_table:
            row_list = row.text.split("\n")
            print(len(row_list))
            if len(row_list) == 6:
                ex_dividend.append(row_list[2])
                payoutDate.append(row_list[3])
                cash_amount.append(row_list[4])
                percent_change.append(row_list[5])
                symbols.append(symbol)

            df = pd.DataFrame(symbols, columns=['Symbols'])
            df[['ExDividend Date']] = ex_dividend
            df[['Payout Date']] = payoutDate
            df[['Cash Amount']] = cash_amount
            df[['Percent Change']] = percent_change
 
            print(df)

            print(symbol.capitalize())


    def get_stock_time_series_data(self, symbol="T", interval="1min", type="TIME_SERIES_INTRADAY"): 
        """
        Gathers stock intraday data from AlphaVantage. 
        Parameters: 
            symbol = stock symbol to gather
            interval = time interval between two consecutive data points in the time series. The following values are supported: 1min, 5min, 15min, 30min, 60min.
            type = the time series of your choice. Please see below for all options:
                TIME_SERIES_INTRADAY_EXTENDED - This API returns the most recent 1-2 months of intraday data and is best suited for short-term/medium-term charting and trading strategy development.
                TIME_SERIES_DAILY - This API returns historical intraday time series for the trailing 2 years, covering over 2 million data points per ticker. 
                TIME_SERIES_DAILY_ADJUSTED - This API returns raw (as-traded) daily time series (date, daily open, daily high, daily low, daily close, daily volume) of the global equity specified, covering 20+ years of historical data. 
                TIME_SERIES_WEEKLY - This API returns weekly time series (last trading day of each week, weekly open, weekly high, weekly low, weekly close, weekly volume) of the global equity specified, covering 20+ years of historical data. 
                TIME_SERIES_WEEKLY_ADJUSTED - This API returns weekly adjusted time series (last trading day of each week, weekly open, weekly high, weekly low, weekly close, weekly adjusted close, weekly volume, weekly dividend) of the global equity specified, covering 20+ years of historical data. 
                TIME_SERIES_MONTHLY - This API returns monthly time series (last trading day of each month, monthly open, monthly high, monthly low, monthly close, monthly volume) of the global equity specified, covering 20+ years of historical data. 
                TIME_SERIES_MONTHLY_ADJUSTED - This API returns monthly adjusted time series (last trading day of each month, monthly open, monthly high, monthly low, monthly close, monthly adjusted close, monthly volume, monthly dividend) of the equity specified, covering 20+ years of historical data. 
        """

        res = requests.get("https://www.alphavantage.co/query?function=" + type + "&symbol=" + symbol + "&interval=" + interval  + "&apikey=" + self.api_key)

        time_stamps = []
        open = []
        high = []
        low = []
        close = []
        volume = []
        stocks = []
        stock_types = []
        headers = ["Time Stamp", "Open", "High", "Low", "Close", "Volume"]

        try:
            conn = sqlite3.connect(self.db_filename)
            c = conn.cursor()

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
                stocks.append(symbol)
                stock_types.append(symbol_type)
                time_stamps.append(row[0])
                open.append(row[1]['1. open'])
                high.append(row[1]['2. high'])
                low.append(row[1]['3. low'])
                close.append(row[1]['4. close'])
                volume.append(row[1]['5. volume'])
    
            df = pd.DataFrame(stocks, columns=['Symbols'])
            df[['Stock Type']] = stock_types
            df[[headers[0]]] = time_stamps
            df[[headers[1]]] = open
            df[[headers[2]]] = high
            df[[headers[3]]] = low
            df[[headers[4]]] = close
            df[[headers[5]]] = volume
            print(df)

            print(symbol.capitalize())
            df.to_sql('STOCK_DATA', conn, if_exists='append', index = False)
            conn.commit()
            conn.close()
        except Exception:
            pass



def main():
    ### DATA IMPORT ###
    symbol_list = []
    #Import all stock data from Alex
    div_stocks = np.genfromtxt('StockData/Watchlist_DIV+Swings_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1, usecols=(0))
    pref_stocks = np.genfromtxt('StockData/Watchlist_Preferred+Stocks_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1, usecols=(0))
    risk_swing_stocks = np.genfromtxt('StockData/Watchlist_Risk+Swing_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1, usecols=(0))
    stock_type_lists = [div_stocks, pref_stocks, risk_swing_stocks]

    for list in stock_type_lists:
        for symbol in list:
            symbol_list.append(symbol)

    dg = data_collector()
    dg.set_symbol_list(symbol_list)
    dg.set_db_filename('stock_data.db')
    dg.create_db()
    dg.get_stock_information_list()
    dg.get_stock_time_series_data()

    for symbol in dg.get_symbol_list():
        dg.get_stock_time_series_data(symbol, '1min', 'TIME_SERIES_INTRADAY_EXTENDED')



if __name__ == '__main__': 
    main()




