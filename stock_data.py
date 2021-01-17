import requests
import numpy as np
import json
import pandas as pd
import sqlite3
import time
import csv
from bs4 import BeautifulSoup
import settings

class data_scraper():
    def __init__(self):
        self.symbol_list = settings.my_database['symbol_list']
        self.db_filename = settings.my_database['filename']
        self.api_key = settings.api_key
        self.table_names = settings.my_database['table_names']

    def create_db(self):
        try:
            conn = sqlite3.connect(self.db_filename)
            c = conn.cursor()
            # Create a table to store stock information
            c.execute('CREATE TABLE STOCK_INFO (Symbol text, Sector text, DividendYield decimal, DividendDate datetime, ExDividendDate datetime)')
            # Create a table to store stock intraday data
            c.execute('CREATE TABLE STOCK_DATA (Symbol text, TimeStamp datetime, Open decimal, High decimal, Low decimal, Close decimal, Volume int)')
            # Create a table to store stock dividend history data
            c.execute('CREATE TABLE STOCK_DIVIDEND_HISTORY (Symbol text, PayoutDate datetime, ExDividendDate datetime, CashAmount decimal, PercentChange decimal)')
            conn.commit()
        except Exception as e:
            print(e)
        finally:
            if conn:
                conn.close()

    def clean_database(self):
        try:
            conn = sqlite3.connect(self.db_filename)
            c = conn.cursor()
            c.execute(  'SELECT Symbol, TimeStamp, COUNT(*)'+
                        'FROM STOCK_DATA' +
                        'GROUP BY TimeStamp' +
                        'HAVING COUNT (TimeStamp)>1')
            results = c.fetchall()

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
            if len(row_list) == 6:
                ex_dividend.append(row_list[1])
                payoutDate.append(row_list[2])
                cash_amount.append(row_list[3])
                percent_change.append(row_list[4])
                symbols.append(symbol)

                df = pd.DataFrame(symbols, columns=['Symbol'])
                df[['ExDividendDate']] = ex_dividend
                df[['PayoutDate']] = payoutDate
                df[['CashAmount']] = cash_amount
                df[['PercentChange']] = percent_change
        conn = sqlite3.connect(self.db_filename)
        df.to_sql('STOCK_DIVIDEND_HISTORY', conn, if_exists='append', index = False)
        conn.commit()
        conn.close()


    def get_stock_time_series_data(self, symbol="T", interval="15min", type="TIME_SERIES_INTRADAY_EXTENDED", month=1, year=1): 
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
        link = str("https://www.alphavantage.co/query?function=" + type + "&symbol=" + symbol + "&interval=" + interval + "&slice=year" + str(year) + "month" + str(month) + "&apikey=" + self.api_key)
        try:
            conn = sqlite3.connect(self.db_filename)
            c = conn.cursor()
            with requests.Session() as s:
                download = s.get(link)
                tic = time.perf_counter()
                decoded_content = download.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                my_list = list(cr)
                for row in my_list:
                    if "time" not in row[0]:
                        row.insert(0, symbol)
                        c.execute('INSERT INTO STOCK_DATA (Symbol,TimeStamp,Open,High,Low,Close,Volume) VALUES (?, ?, ?, ?, ?, ?, ?)', row)
                        print(row)
                        conn.commit()         
            conn.close()
            toc = time.perf_counter()
            if (toc-tic < 12):
                time.sleep(12-(toc-tic))
        except Exception as e:
            print(e)
            pass

    def get_all_stock_time_series_data(self, symbol="T", interval="1min", type="TIME_SERIES_INTRADAY_EXTENDED"): 
        years = range(1,10,1)
        months = range(1,12,1)
        for year in years:
            for month in months:
                self.get_stock_time_series_data(symbol, interval, type, month, year)

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

    dg = data_scraper()
    #dg.set_symbol_list(symbol_list)
    #symbol = "T"
    dg.set_db_filename('stock_data.db')
    #dg.get_stock_information_single(symbol)
    #dg.get_all_stock_time_series_data(symbol)
    #dg.get_dividend_history(symbol)
    dg.clean_database()




if __name__ == '__main__': 
    main()




