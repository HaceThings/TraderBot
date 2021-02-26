import settings
import stock_data
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import time
import datetime


symbol_list = ['VLYPO']
#Import all stock data from Alex
#div_stocks = np.genfromtxt('StockData/Watchlist_DIV+Swings_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1, usecols=(0))
#pref_stocks = np.genfromtxt('StockData/Watchlist_Preferred+Stocks_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1, usecols=(0))
#risk_swing_stocks = np.genfromtxt('StockData/Watchlist_Risk+Swing_2020_12_30.csv', delimiter=',', dtype=str, skip_header=1, usecols=(0))
#stock_type_lists = [div_stocks, pref_stocks, risk_swing_stocks]
#stock_type_lists = div_stocks

#for list in stock_type_lists:
    #symbol_list.append(list)

db = stock_data.database_manager()
div_data = stock_data.dividend_data()

id_d = stock_data.intraday_data()
                
for stock in symbol_list:
    print(stock)
    div_data.set_dividend_history(stock)
    df_div = div_data.get_dividend_history(stock)
    

    if (stock.find('-') == -1):
        id_d.set_intraday_data(symbol=stock, type='TIME_SERIES_DAILY', output_size='full', interval='60min')
        time.sleep(10)

    fig, ax = plt.subplots()

    for date in df_div['ExDividendDate']:
        try:
            date = date + " 00:00:00"
            exdiv_date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
            delta_t = datetime.timedelta(weeks=4)

            start_date = exdiv_date - delta_t
            end_date = exdiv_date + delta_t

            if (end_date < datetime.datetime.today()):
                payout_data = id_d.get_stock_time_series_data(symbol=stock, date_range= True, start_date=start_date, end_date=end_date)

                x = range(0, len(payout_data.index), 1)
                payout_data['count'] = x

                y = payout_data['Open'] / payout_data['Open'][exdiv_date.strftime('%Y-%m-%d %H:%M:%S')]
            
                #ax.plot(payout_data.index, payout_data['High'])
                ax.plot(x, payout_data['Open'], label=date)
        except Exception as e:
            print(e)
    
    plt.axvline(x=payout_data['count'][exdiv_date.strftime('%Y-%m-%d %H:%M:%S')], color='b', label='Ex Dividend Date')
    
    ax.set(xlabel='Normalized Dates', ylabel='Normalized Stock Price, compared to payout price', title = stock)
    ax.grid()
    ax.legend()

    plt.show()
    
#Get dividend dates from dividend data
#Sort through and find the dates when the exdividend occurred for each stock
