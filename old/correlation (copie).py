#!/usr/bin/env python3
# coding: utf-8 

import asyncio
import os
import ccxt
import json
import time
import calendar
from datetime import datetime, date
import sqlite3
import warnings

import pandas as pd
from statsmodels.tsa.stattools import coint

################################ global constants ################################
global PATH_EXCH_INFO

PATH_EXCH_INFO = '../Exchange_info/'
################################ main ################################

def main():

    #Init
    t0 = time.time()
    conn = sqlite3.connect('arbot.db')
    cursor = conn.cursor()
    exch_list = ['binance', 'bitfinex', 'kraken']
    exch = {}
    for i in exch_list:
        exch[i] = eval('ccxt.%s()' % (i,))

    check_update_exch_info(exch,1)

    print(tstamp_s_localdate('2020-04-21 00:00:00'))

    new_date = int(tstamp_s_localdate('2020-04-20 00:00:00')*1000)
    old_date = int(tstamp_s_localdate('2020-04-01 00:00:00')*1000)




    base_list = ['ETH','TRX','BNB','DOGE','XMR','DASH','LTC']
    quote = 'USDT'
    pair_list = []

    for i in base_list:
        pair_list.append(i+'/'+quote)

    dict_corr = {}
    for i in base_list:
        table_name = get_table_name(exch['binance'],i+'/'+quote,'1h')
        dict_corr[i]=pd.Series(select_data_tstamp(table_name, new_date, old_date, cursor))
        print(len(dict_corr[i]))
        print(((new_date - old_date) // convert_to_ms('1h')) + 1)
    df = pd.DataFrame(dict_corr)
    print(df)

    for i in list(df):
        for j in list(df):
            if i != j:
                pair = i+'-'+j
                coint_result = coint(df[i],df[j])
                print(pair, coint_result)

    # ETH - TRX(-0.6488947656936879, 0.951877167975573, array([-3.92061856, -3.34956216, -3.05376396]))
    # refer to https://www.statsmodels.org/stable/generated/statsmodels.tsa.stattools.coint.html




    # corr_matrix = df.diff().corr()
    # print(corr_matrix)



    conn.commit()  # Save arbot database changes
    conn.close()  # Closes arbot database

    print(MyWarning.number_warning, 'warnings')
    print('Time prog secs','%.2f'%(time.time()-t0))

    # base_list = ['ETH','TRX','BNB','DOGE','XMR','DASH','LTC']
    # quote = 'USDT'
    # pair_list = []
    # for i in base_list:
    #     pair_list.append(i+'/'+quote)
    #
    # dict_corr = {}
    # for i in pair_list:
    #      dict_corr[i]=pd.Series(select_data_db(exch['binance'], i, '5m', cursor))
    # df = pd.DataFrame(dict_corr)
    # print(df)
    # corr_matrix = df.diff().corr()
    # print(corr_matrix)


################################ class ################################

class MyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class MyWarning(UserWarning):
    number_warning = 0
    def __init__(self, value):
        self.value = value
        warnings.warn(value)
        MyWarning.number_warning += 1

################################ functions ################################
def get_all_historical(exchang_list:list):
    t_frame_list = [['1m','2020-04-19T00:00:00Z'],['5m','2020-03-21T00:00:00Z']]
    # oldest_date = ['2020-04-19T00:00:00Z','2020-03-21T00:00:00Z']
    t_frame_list.reverse()
    # oldest_date.reverse()

    print(t_frame_list)

    info_binance = read_json('../Exchange_info/binance.json')
    binance_symbol =[]

    # for symbol, j in info_binance.items():
    #     table_name = get_table_name(exch['binance'],symbol,'3d')
    #     drop_table_db(table_name,cursor)

    time_matrix = {}
    for t_frame in t_frame_list:
        tloop=time.time()
        nb_pair = 0
        nb_t_frame =0
        for symbol,j in info_binance.items():
            get_ohlcv_db_v2(exch['binance'], symbol, t_frame[0], time.time(), t_frame[1], cursor)
            conn.commit()
            nb_pair += 1
            print('Pairs:',nb_pair,'/',len(info_binance))
            print('Time:', time.time() - tloop)
        print('timeframes:',nb_t_frame,'/',len(time_matrix))
        time_matrix[t_frame[0]]=time.time()-tloop

        for i in time_matrix:
            print(i)

def get_ohlcv_db_v2(exchange :object, symbol:str, t_frame:str,tstamp_s_max:int, old_date:str,cursor:object):
    table_name = get_table_name(exchange, symbol, t_frame)

    tstamp_ms_max = tstamp_s_max*1000
    tstamp_ms_min = exchange.parse8601(old_date)   #format old_date '2020-04-01T00:00:00Z'

    #Check if a table has aldready been created and fills it if not
    #Check if that table has aldready been filled and fills it if not
    if not (table_exists_db(table_name, cursor)) or not (data_exists_db(table_name, cursor)) :
        data = get_ohlcv_v2(exchange, symbol, t_frame, tstamp_ms_min)
        if data != 0:
            create_table_db(table_name, cursor)
            # Insert data into db
            insert_pair_db(table_name, data, cursor)
            print('inside 1')
            return len(data)
        else:
            return -1

    t_frame_ms=convert_to_ms(t_frame)

    # Get the oldest and newest row time.
    max_tstamp_ms_db = max_column_db(table_name, 'tstamp', cursor)
    min_tstamp_ms_db = min_column_db(table_name, 'tstamp', cursor)

    if((min_tstamp_ms_db-tstamp_ms_min)>t_frame_ms) or ((tstamp_ms_max - max_tstamp_ms_db)>t_frame_ms):
        #check oldest row in database
        if tstamp_ms_min < min_tstamp_ms_db:
            # delete all data in the table
            delete_all_row_db(table_name, cursor)
            # Fetch again all data from exchange server
            data = get_ohlcv_v2(exchange, symbol, t_frame, tstamp_ms_min)
            if data != 0:
                create_table_db(table_name, cursor)
                # Insert data into db
                insert_pair_db(table_name, data, cursor)
                print('inside 2')
                return len(data)
            else:
                return -1
        elif tstamp_ms_max > max_tstamp_ms_db:
            data = get_ohlcv_v2(exchange, symbol, t_frame, max_tstamp_ms_db+t_frame_ms)
            if data != 0:
                create_table_db(table_name, cursor)
                # Insert data into db
                insert_pair_db(table_name, data, cursor)
                print('inside 3')
                return len(data)
            else:
                return -1
    else:
        print('all data aldready in database:',table_name)
        return 0

def get_ohlcv_v2(exchange :object, symbol:str, t_frame:str,since:int):
    if exchange.has['fetchOHLCV']:
        #check the t_frame parameter in timeframes list of the exchange
        if not(t_frame in exchange.timeframes.keys()):
            raise MyError('t_frame is not in ' + exchange.id + ' timeframes.')

        buff_time = exchange.milliseconds()
        # since = buff_time - n_days * 86400000  # -1 day from now # since = exchange.parse8601('2020-04-01T00:00:00Z')

        time_loop = since
        #Pagination method to fetch historial OHLCV
        all_OHLCV = []
        while time_loop < buff_time:
            OHLCV = exchange.fetch_ohlcv(symbol, t_frame, time_loop)
            if len(OHLCV):
                time_loop = OHLCV[len(OHLCV) - 1][0] + convert_to_ms(t_frame)  # widhdraw t_frame
                all_OHLCV += OHLCV
                print('Fetching:',symbol,t_frame,len(all_OHLCV))
            else:
                break
            time.sleep(exchange.rateLimit / 1000)

        #Data lenght check
        print('all_OHLCV:',len(all_OHLCV))
        print('taill calc:',((buff_time - since) // (convert_to_ms(t_frame))) + 1)

        if len(all_OHLCV) == ((buff_time - since)//(convert_to_ms(t_frame)))+1:
            return all_OHLCV
        else:
            MyWarning('data lenght asked does not match the data returned')
            return all_OHLCV
    else:
        MyWarning('No fetchOHLCV available for: ' + exchange.id)
        return 0

def get_ohlcv_db(exchange :object, symbol:str, t_frame:str, n_days:int,cursor:object):

    # Fetch data from exchange server
    data = get_ohlcv(exchange, symbol, t_frame, n_days)
    if data != 0:
        #Get the pair_id from exchange.json in PATH_EXCH_INFO folder
        exchange_info=read_json(PATH_EXCH_INFO + exchange.id + '.json')
        pair_id = exchange_info[symbol]['id']

        #Name table : exchange_symbol_timeframe
        table_name = exchange.id + '_' + pair_id + '_' + t_frame
        create_table_db(table_name, cursor)

        # Insert data into db
        insert_pair_db(table_name,data,cursor)
        return 1
    else:
        return 0

def get_ohlcv(exchange :object, symbol:str, t_frame:str, n_days:int):
    if exchange.has['fetchOHLCV']:
        #check the t_frame parameter in timeframes list of the exchange
        if not(t_frame in exchange.timeframes.keys()):
            raise MyError('t_frame is not in ' + exchange.id + ' timeframes.')

        buff_time = exchange.milliseconds()
        since = buff_time - n_days * 86400000  # -1 day from now # since = exchange.parse8601('2020-04-01T00:00:00Z')

        #Pagination method to fetch historial OHLCV
        all_OHLCV = []
        while since < (buff_time):
            OHLCV = exchange.fetch_ohlcv(symbol, t_frame, since)
            if len(OHLCV):
                since = OHLCV[len(OHLCV) - 1][0] + convert_to_ms(t_frame)  # widhdraw t_frame
                all_OHLCV += OHLCV
            else:
                break
            time.sleep(exchange.rateLimit / 1000)

        #Data lenght check
        if len(all_OHLCV) != n_days:
            return all_OHLCV
        else:
            raise MyError('data lenght asked does not match the data returned')
            return 0
    else:
        MyWarning('No fetchOHLCV available for: ' + exchange.id)
        return 0

# Gives a list of all methods of the object
def list_meth(obj:object):
    for i in dir(obj):
        print(i)

# Gives a list of all attributes of the object
def list_attr(obj:object):
    for key, value in obj.__dict__.items():
        print(key, ":", value)

# Converts a dictionary into a json file
def json_file(filename:str, data:list):
    filename_ext = filename
    with open(filename_ext, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=1)

def read_json(filename:str)->list:
    filename_ext = filename
    with open(filename_ext, 'r') as f:
        dict = json.load(f)
    return dict

def convert_to_ms(t_frame:str)->int:
    t_frame_list = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1M']
    if (t_frame in t_frame_list):

        buff_str = t_frame[len(t_frame) - 1]
        buff_int = int(t_frame[0:len(t_frame) - 1])

        if buff_str == 'm':
            return buff_int * 60 * 1000
        elif buff_str == 'h':
            return buff_int * 3600 * 1000
        elif buff_str == 'd':
            return buff_int * 3600 * 24 * 1000
        elif buff_str == 'M':
            return buff_int * 3600 * 24 * 31 * 1000
        else:
            raise MyError('convert_to_ms() has a problem')
    else:
        raise MyError('Parameter gaptime of convert_to_ms() is not in t_frame_list')

def check_update_exch_info(exch:list, n_days:int):
    no_update = 1
    for key,val in exch.items():
        if os.path.isfile('Exchange_info/' + key + '.json'):
            filename='Exchange_info/' + key + '.json'
            if time.time() - os.path.getmtime('Exchange_info/' + key + '.json') > n_days*3600*24:
                json_file(filename,val.load_markets())
                no_update = 0
                print('Update of: '+filename)
        else:
            print('Creation of:'+filename)
            json_file(filename, val.load_markets())
            no_update = 0
    if no_update:
        print('Exchange_info was aldready up to date')

def tstamp_s_localdate(localdate:str):
    return time.mktime(datetime.strptime(localdate, '%Y-%m-%d %H:%M:%S').timetuple())

# DATA BASE FUNCTIONS
def create_table_db(table_name:str, cursor:object):
    print(table_exists_db(table_name,cursor))
    if not table_exists_db(table_name,cursor):
        sql = """CREATE TABLE IF NOT EXISTS %s(
                 tstamp INTEGER PRIMARY KEY,
                 open REAL,
                 high REAL,
                 low REAL,
                 close REAL,
                 volume REAL
            )
            """ % (table_name)
        cursor.execute(sql)
        print('table : %s created with sucess' % (table_name))

def insert_pair_db(table_name,data:list, cursor:object):
    sql="""INSERT INTO %s(tstamp, open, high, low, close, volume) VALUES(?, ?, ?, ?, ?, ?)"""% (table_name)
    cursor.executemany(sql, data)
    print('Data inserted with success.')

def delete_all_row_db(table_name:str,cursor:object):
    if data_exists_db(table_name,cursor):
        sql="""DELETE from %s
                      WHERE EXISTS( SELECT *
                                    FROM %s)
                   """%(table_name,table_name)
        cursor.execute(sql)

def delete_last_6d_row_db(table_name:str, cursor:object):
    limit= time.time()*1000-6*convert_to_ms('1d')
    if data_exists_db(table_name,cursor):
        sql="""DELETE from %s
               WHERE (tstamp >= %s)
                   """%(table_name,limit)
        cursor.execute(sql)

def select_data_tstamp(table_name:str,tstamp_ms_new:int,tstamp_ms_old,cursor:object):
    sql="""SELECT close FROM %s WHERE ( tstamp <= ? AND tstamp >= ?)"""%(table_name)
    rows = cursor.execute(sql,(tstamp_ms_new,tstamp_ms_old))
    data=[]
    for row in rows:
        data.append(row[0])
    return data

def select_data_db(table_name:str,cursor:object):
    sql="""SELECT close FROM %s"""%(table_name)
    rows=cursor.execute(sql)
    data=[]
    for row in rows:
        data.append(row[0])
    return data

def min_column_db(table_name:str,column:str,cursor:object)->int:
    sql="""SELECT tstamp FROM %s WHERE tstamp = ( SELECT min(%s) From %s)"""%(table_name,column,table_name)
    rows=cursor.execute(sql)
    data=[]
    for row in rows:
        data =row[0]
    return data

def max_column_db(table_name:str,column:str,cursor:object)->int:
    sql="""SELECT tstamp FROM %s WHERE tstamp = ( SELECT max(%s) From %s)"""%(table_name,column,table_name)
    rows=cursor.execute(sql)
    data=[]
    for row in rows:
        data =row[0]
    return data

def table_exists_db(table_name:str,cursor:object):
    sql=""" SELECT count(name) FROM sqlite_master WHERE type='table' AND name='%s' """%(table_name)
    cursor.execute(sql)
    return cursor.fetchone()[0]

def data_exists_db(table_name:str,cursor:object):
    sql="""SELECT count(*) FROM(SELECT 0 FROM %s LIMIT 1)"""%(table_name)
    cursor.execute(sql)
    return cursor.fetchone()[0]

def drop_table_db(table_name:str,cursor:object):
    cursor.execute("""DROP TABLE %s""" % (table_name))

def get_table_name(exchange:str,symbol:str,t_frame:str)->str:
    # Get the pair_id from exchange.json in PATH_EXCH_INFO folder
    exchange_info = read_json(PATH_EXCH_INFO + exchange.id + '.json')
    pair_id = exchange_info[symbol]['id']

    # Name table : exchange_symbol_timeframe
    table_name = exchange.id + '_' + pair_id + '_' + t_frame

    return table_name


################################ test bench ################################
# This function aims to validate the subfunctions of this program
# if a major change occurded

def test_bench1():
    conn = sqlite3.connect('test_bench.db')
    cursor = conn.cursor()

    print('\n########TESTS########>:\n')

    exchange = exch['binance']
    symbol = 'BTC/USDT'
    t_frame = '1d'
    newest_date = time.time()
    oldest_date = '2020-04-01T00:00:00Z'
    table_name = get_table_name(exchange, symbol, t_frame)

    get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor)

    print("test : delete row + add data")
    delete_all_row_db('binance_BTCUSDT_1d', cursor)
    get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor)
    print('test ok\n')

    print('test : drop table + add data')
    cursor.execute("""DROP TABLE %s""" % (table_name))
    get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor)
    print('test ok\n')

    # Remplacer 52 par le nb de jour au lancement du test
    print('test : get old data - 31 days')
    oldest_date = '2020-03-01T00:00:00Z'
    if get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor) == 52:
        print('test ok\n')
    else:
        print('test ko\n')

    print('test : fetch only newest data not in database')
    delete_last_6d_row_db(table_name, cursor)
    if get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor) == 6:
        print('test ok\n')
    else:
        print('test ko\n')

    t_frame = '5m'
    oldest_date = '2020-03-11T00:00:00Z'
    table_name2 = get_table_name(exchange, symbol, t_frame)

    print('test : timeframe = 5m')
    oldest_date = '2020-03-11T00:00:00Z'
    print('data_lenght:', get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor))

    print('test : timeframe = 5m, drop 5days of data')
    delete_last_6d_row_db(table_name2, cursor)
    print('data_lenght:', get_ohlcv_db_v2(exchange, symbol, t_frame, newest_date, oldest_date, cursor))

    print('test: fetch only new and old data not in database')

    print('clean data base')
    cursor.execute("""DROP TABLE %s""" % (table_name))
    cursor.execute("""DROP TABLE %s""" % (table_name2))

    conn.commit()  # Save arbot database changes
    conn.close()  # Closes arbot database

################################ run ################################

main()






# print(BTCUSD_1h)
# print(len(BTCUSD_1h))

# if binance_exch.has['fetchOHLCV']:
#     json_file('BTCUSD_1h',binance_exch.fetch_ohlcv('BTC/USD', '1h', ts))

# print(json.dumps(binance_markets, indent=1))
# json_file('binance_exchange', binance_exch.load_markets())
# exchange_list = ccxt.exchanges
# print(binance_exch.id)
# print(binance_markets[0])

# if binance_exch.has['fetchOHLCV']:
#     BTCUSD_1d=binance_exch.fetch_ohlcv('BTC/USD',ts,'1d')

# ohlcv_date = time.strptime('2019 11 22','%Y %m %d')
# print(time.mktime(ohlcv_date))


################
# USEFUL STUFF #
################


# Gives the OHLCV of all symbols of a market
#
# if binance_exch.has['fetchOHLCV']:
#     for symbol in binance_exch.markets:
#         time.sleep (exchange.rateLimit / 1000) # time.sleep wants seconds
#         print (symbol, exchange.fetch_ohlcv (symbol, '1d')) # one day, since parameter can be added


# list_meth(binance_exch)
# list_attr(binance_exch)

################# Time #################

# #Tuple d is in utc
# from datetime import datetime, date
# import calendar
# timestamp1 = calendar.timegm(d.timetuple())
# datetime.utcfromtimestamp(timestamp1)
#
# #Tuple d is in local timezone
# import time
# timestamp2 = time.mktime(d.timetuple()) # DO NOT USE IT WITH UTC DATE
# datetime.fromtimestamp(timestamp2)


# dt = datetime.strptime('2020-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')
# ts = calendar.timegm(dt.timetuple())
# print(ts)