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
        settings.Settings().set('filename', self.db_filename)

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
        soup = BeautifulSoup(res.text, 'html.parser')

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
        try:
            sql_query = pd.read_sql_query("""SELECT DISTINCT * FROM STOCK_DIVIDEND_HISTORY WHERE Symbol = ?""", params = [symbol], con=conn)
            self.data = pd.DataFrame(sql_query, columns=['Symbol', 'ExDividendDate', 'PayoutDate', 'CashAmount', 'PercentChange'])
            return self.data
            
        except Exception as e:
            print(e)
            pass

        finally:
            if conn:
                conn.close()

class intraday_data():
    def __init__(self):
        self.db_filename = settings.Settings()._dict_['filename']
        self.api_key = settings.Settings()._dict_['api_key']
        self.stock_information_data = []
        self.stock_intraday_data = []

    def set_db_filename(self, filename):
        self.db_filename = filename

    def get_stock_information(self, symbol):
        try:
            conn = sqlite3.connect(self.db_filename)
            sql_query = pd.read_sql_query("""SELECT DISTINCT * FROM STOCK_INFO WHERE Symbol = ?""", con=conn, params=[symbol])
            self.data = pd.DataFrame(sql_query, columns=['Symbol', 'Sector', 'DividendYield', 'DividendDate', 'ExDividendDate'])
            
        except Exception as e:
            print(e)
            pass

        finally:
            if conn:
                conn.close()

    def set_stock_information(self, symbol):
        res = requests.get("https://www.alphavantage.co/query?function=" + "OVERVIEW" + "&symbol=" + symbol + "&apikey=" + self.api_key)
        res_data = json.loads(res.text)
         
        try:
            conn = sqlite3.connect(self.db_filename)
            c = conn.cursor()
            data = [res_data['Symbol'], res_data['DividendYield'], res_data['DividendDate'], res_data['ExDividendDate'], res_data['DividendPerShare']]
            c.execute('INSERT INTO STOCK_INFO (Symbol,Sector,DividendYield,DividendDate,ExDividendDate) VALUES (?, ?, ?, ?, ?)', data)
            conn.commit()

        except Exception as e:
            print(e)
            pass

        finally:
            if conn:
                conn.close()

    def set_intraday_extended_data(self, symbol="T", interval="15min", month_range = range(1,12,1), year_range=range(1,2,1)): 
        """
        Pulls extended stock intraday data from AlphaVantage. Use get_stock_time_series_data to get the intraday data.
        :param symbol: Stock symbol
        :param interval: time interval between two consecutive data points in the time series. The following values are supported: 1min, 5min, 15min, 30min, 60min.
        :param month_range: month range that will be pulled from AlphaVantage. Default value should handle almost all cases.
        :param year_range: year range that will be pulled from AlphaVantage. Default value should handle almost all cases.   
        """

        for year in year_range:
            for month in month_range:
                
                slice = "year" + year + "month" + month
                type = "TIME_SERIES_INTRADAY_EXTENDED"
                link = str("https://www.alphavantage.co/query?function=" + type + "&symbol=" + symbol + "&interval=" + interval + "&slice=" + slice + "&apikey=" + self.api_key)
                
                try:
                    conn = sqlite3.connect(self.db_filename)
                    df = pd.read_csv(link, ',', header = [0], parse_dates=['timestamp'])
                    df.set_index('TimeStamp', inplace=True)
                    df.to_sql(self.db_filename, conn, if_exists='append')
                    conn.commit()
                    print("Data has been pulled successfully. Please get the data from the get_stock_time_series_data method.")
                except Exception as e:
                    print(e)
                    pass

                finally:
                    if conn:
                        conn.close()

    def set_intraday_data(self,  symbol="T", interval="15min", output_size="compact", type="TIME_SERIES_INTRADAY", ):
        """
        Pulls stock intraday data from AlphaVantage. Use get_stock_time_series_data to get the intraday data.
        :param symbol: Stock symbol
        :param interval: time interval between two consecutive data points in the time series. The following values are supported: 1min, 5min, 15min, 30min, 60min.
        :param output_size: determines how much data is pulled. "compact" only pulls the past 100 data points. "full" pulls the full-length intraday time series.
        :param type = the time series of your choice. Please see below for all options:
                TIME_SERIES_DAILY - This API returns historical intraday time series for the trailing 2 years, covering over 2 million data points per ticker. 
                TIME_SERIES_DAILY_ADJUSTED - This API returns raw (as-traded) daily time series (date, daily open, daily high, daily low, daily close, daily volume) of the global equity specified, covering 20+ years of historical data. 
                TIME_SERIES_WEEKLY - This API returns weekly time series (last trading day of each week, weekly open, weekly high, weekly low, weekly close, weekly volume) of the global equity specified, covering 20+ years of historical data. 
                TIME_SERIES_WEEKLY_ADJUSTED - This API returns weekly adjusted time series (last trading day of each week, weekly open, weekly high, weekly low, weekly close, weekly adjusted close, weekly volume, weekly dividend) of the global equity specified, covering 20+ years of historical data. 
                TIME_SERIES_MONTHLY - This API returns monthly time series (last trading day of each month, monthly open, monthly high, monthly low, monthly close, monthly volume) of the global equity specified, covering 20+ years of historical data. 
                TIME_SERIES_MONTHLY_ADJUSTED - This API returns monthly adjusted time series (last trading day of each month, monthly open, monthly high, monthly low, monthly close, monthly adjusted close, monthly volume, monthly dividend) of the equity specified, covering 20+ years of historical data. 
        """
            
        link = str("https://www.alphavantage.co/query?function=" + type + "&symbol=" + symbol + "&interval=" + interval + "&outputsize=" + output_size + "&apikey=" + self.api_key + "&datatype=csv")
        try:
            conn = sqlite3.connect(self.db_filename)
            df = pd.read_csv(link, ',', header = [0], parse_dates=['timestamp'])
            df.set_index('timestamp', inplace = True)

            df.to_sql(symbol, conn, if_exists='append')
            conn.commit()
            print("Data has been pulled successfully. Please get the data from the get_stock_time_series_data method.")
        except Exception as e:
            print(e)
            pass

        finally:
            if conn:
                conn.close()

    def get_stock_time_series_data(self, symbol="T", start_date = "2021-01-28 11:00:00", end_date = "2021-01-28 15:00:00", freq = 'T'):
        """
        Return a pandas dataframe object with a stock's intraday data in a given date range and frequency.
        :param symbol: Stock symbol
        :param start_date: Start of time series data range
        :param end_date: End of time series data range
        :param freq: Frequency between data points. Common values: "D" = day, "H" = hour, "T" = minute, "W" = week
        """
        try:
            conn = sqlite3.connect(self.db_filename)
            query = ("SELECT DISTINCT * FROM " + symbol + " WHERE timestamp BETWEEN '" + start_date + "' AND '" + end_date + "'")
            
            df = pd.read_sql(query, conn, parse_dates = ['timestamp'])
            
            # TODO: Need to change the names of each column to match the format for backtesting.
            # df.columns[1].name = "Open"
            # df.columns[2].name = "High"
            # df.columns[3].name = "Low"
            # df.columns[4].name = "Close"
            # df.columns[5].name = "Volume"
            
            # TODO: Resample data. Use the resample method in DataFrame.

            # df.resample(freq).mean()
            return df

        except Exception as e:
            print(e)
            pass
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    import stock_data
    import pandas as pd
    from backtesting import Backtest, Strategy
    db = stock_data.database_manager()

    div_data = stock_data.dividend_data()
    div_data.set_dividend_history("T")
    df_div = div_data.get_dividend_history("T")

    id_d = stock_data.intraday_data()
    id_d.set_intraday_data()
    stocks = id_d.get_stock_time_series_data()

    print(df_div)
    print(stocks)

    stocks.plot(figsize=(10,6))
    
    class SmaCross(Strategy):
        def init(self):
            price = self.data.Close

        def next(self):
            pass

    bt = Backtest(stocks, SmaCross)
    stats = bt.run()
    bt.plot()

    matplotlib.pyplot.show()