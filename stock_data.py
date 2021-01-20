import requests
import numpy as np
import json
import pandas as pd
import sqlite3
import time
import csv
from bs4 import BeautifulSoup
import settings
import matplotlib

class database_manager():
    def __init__(self):
        self.db_filename = settings.Settings()._dict_['filename']

    def get_db_filename(self):
        return self.db_filename

    def set_db_filename(self, new_filename):
        self.db_filename = new_filename
        settings.Settings()._dict_['filename'] = self.db_filename

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

class dividend_data():
    def __init__(self):
        self.data = pd.DataFrame()
        self.db_filename = settings.Settings()._dict_['filename']
        self.URL = "https://dividendhistory.org/payout/"

    def set_dividend_history(self, symbol):
        """Gets dividend history for a given stock."""
        ex_dividend = []
        payoutDate = []
        cash_amount = []
        percent_change = []
        symbols = []

        res = requests.get(str(self.URL + symbol + "/"))
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

        self.data = pd.DataFrame(data = {'Symbol' : symbols, 'ExDividendDate' : ex_dividend, 'PayoutDate' : payoutDate, 'CashAmount' : cash_amount, 'PercentChange' : percent_change})

        conn = sqlite3.connect(self.db_filename)
        self.data.to_sql('STOCK_DIVIDEND_HISTORY', conn, if_exists='append', index = False)
        conn.commit()
        conn.close()
        print("Dividend Data has been set.")

    def get_dividend_history(self, symbol):
        conn = sqlite3.connect(self.db_filename)
        c = conn.cursor()
        sql_query = pd.read_sql_query('SELECT ' + symbol + ' FROM STOCK_DIVIDEND_HISTORY')
        self.data = pd.DataFrame(sql_query, columns=['Symbol', 'ExDividendDate', 'PayoutDate', 'CashAmount', 'PercentChange'])
        return self.data

class intraday_data():
    def __init__(self):
        self.db_filename = settings.Settings()._dict_['filename']
        self.api_key = settings.Settings()._dict_['api_key']
        self.stock_information_data = []
        self.stock_intraday_data = []

    def get_stock_information(self):
        conn = sqlite3.connect(self.db_filename)
        sql_query = pd.read_sql_query("""SELECT DISTINCT * FROM STOCK_INFO""")
        self.data = pd.DataFrame(sql_query, columns=['Symbol', 'Sector', 'DividendYield', 'DividendDate', 'ExDividendDate'])

    def set_stock_information(self, symbol):
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

    def set_stock_time_series_data(self, symbol="T", interval="15min", type="TIME_SERIES_INTRADAY_EXTENDED", month=1, year=1): 
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

    def set_stock_time_series_data_extended(self, symbol="T", interval="1min", type="TIME_SERIES_INTRADAY_EXTENDED"): 
            years = range(1,10,1)
            months = range(1,12,1)
            for year in years:
                for month in months:
                    self.set_stock_time_series_data(symbol, interval, type, month, year)

    def get_stock_time_series_data(self, symbol="T", interval="15min", type="TIME_SERIES_INTRADAY_EXTENDED", start_date = "2020-12-31 08:00:00", end_date = "2020-12-31 15:00:00"):
        conn = sqlite3.connect(self.db_filename)
        #Add time interval data
        sql_query = pd.read_sql_query("""SELECT DISTINCT * FROM STOCK_DATA WHERE TimeStamp BETWEEN ? AND ? AND Symbol = ?;""", params = [start_date, end_date, symbol])
        self.data = pd.DataFrame(sql_query, columns=['Symbol', 'TimeStamp', 'Open', 'High', 'Close', 'Volume'])

class plotter():


