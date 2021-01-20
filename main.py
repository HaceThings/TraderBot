import settings
import stock_data
import pandas as pd


def main(): 
    symbol_list = []
    #Import all stock data from Alex
    div_stocks = np.genfromtxt('StockData/Watchlist_DIV+Swings_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1, usecols=(0))
    #pref_stocks = np.genfromtxt('StockData/Watchlist_Preferred+Stocks_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1, usecols=(0))
    #risk_swing_stocks = np.genfromtxt('StockData/Watchlist_Risk+Swing_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1, usecols=(0))
    #stock_type_lists = [div_stocks, pref_stocks, risk_swing_stocks]
    stock_type_lists = div_stocks

    for list in stock_type_lists:
        for symbol in list:
            symbol_list.append(symbol)

    db = stock_data.database_manager()
    db.create_db()

    div_data = stock_data.dividend_data()
    div_data.set_dividend_history(symbol_list[0])
    df_div = div_data.get_dividend_history(symbol_list[0])

    print(df_div)

