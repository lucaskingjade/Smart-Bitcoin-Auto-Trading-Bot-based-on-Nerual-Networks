# -*- coding: utf-8 -*-
"""
Created on Mon Feb 19 13:27:04 2018

@author: v-beshi
"""
import sys
import requests
import json
import time
import pandas as pd
import bfx
import huobi_USDT
import wallstreet_news
from okex2 import OKCoinFuture as ok
import pyodbc
import traceback
mykey=ok('www.okex.com','fabec789-4981-46b7-8155-db0730f6157e','A1C6A6937573B675B519A789C1E77C97')
#replace the Public-Key and Private-Key with your OKex API access
#con=pyodbc.connect('DRIVER={SQL Server};SERVER=server;DATABASE=db;UID=id;PWD=password')
con=pyodbc.connect('DSN=MYAMAZONSQL;UID=lucaskingjade;DATABASE=PythonTestDB;PWD=wq891216',autocommit=True)
#connect to SQL Server Database
def input_data(tt):
    #input the number of rows you want to input
    j=0
    for i in range(0,tt):
        #count number of rows
        try:
            ok0330=float(mykey.future_ticker('btc_usd','quarter')['ticker']['last'])
            ok_thisweek=float(mykey.future_ticker('btc_usd','this_week')['ticker']['last'])
            bfx_bids_wall=bfx.bfx_books()['bids_wall']
            bfx_asks_wall=bfx.bfx_books()['asks_wall']
            bfx_total_bids=bfx.bfx_books()['total_bids']
            bfx_total_asks=bfx.bfx_books()['total_asks']
            bfx_buy_volumn=bfx.bfx_volumn()[0]
            bfx_sell_volumn=bfx.bfx_volumn()[1]
            bfx_last_price=bfx.bfx_ticker()
            exchange_rate=float(mykey.exchange_rate()['rate'])
            huobiUSDT=float(huobi_USDT.get_usdt_price())
            #get USDT price from huobi.pro
            news_emotion=float(wallstreet_news.wallstr_news())
            print('test')
            print(news_emotion)
            #get news emotion from wallstreet news blockchain channel
            cursor=con.cursor()
            print(cursor)
            print('test')
            # db_name = 'PythonTestDB'
            # create_db_sql = 'CREATE DATABASE ' + db_name
            # cursor.execute(create_db_sql)
            # if i==0:
            #     create_table_sql = 'DROP TABLE BitcoinTradeHistory'
            #     cursor.execute(create_table_sql)
            #     string = 'CREATE TABLE BitcoinTradeHistory(date DATE, last_price FLOAT ,ok_thisweek FLOAT ,bfx_bids_wall FLOAT ,bfx_asks_wall FLOAT   ,bfx_total_bids FLOAT   ,bfx_total_asks FLOAT   ,bfx_buy_volumn FLOAT    ,bfx_sell_volumn FLOAT  ,bfx_last_price FLOAT   ,exchange_rate FLOAT    ,huobiUSDT FLOAT    ,news_emotion FLOAT)'
            #     cursor.execute(string)
            cursor.execute("insert into BitcoinTradeHistory values(getdate(),{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11})".format(ok0330,ok_thisweek,bfx_bids_wall,bfx_asks_wall,bfx_total_bids,bfx_total_asks,bfx_buy_volumn,bfx_sell_volumn,bfx_last_price,exchange_rate,huobiUSDT,news_emotion))
            print('test')
            #insert into SQL Server Database
            con.commit()
            j=j+1
            print('collected {0} rows'.format(j))
            time.sleep(60) 
        except:
            print('connect error')
            traceback.print_exc(file=sys.stdout)
    print('done')

if __name__=='__main__':
    input_data(10)
