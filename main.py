#!/usr/bin/env python3
# coding: utf-8 

################################################## global ###################################################
global PATH_DATABASE
global PATH_EXCH_INFO
global PATH_FOLDER_KEY


PATH_DATABASE = '/media/seb/USB1/arbot.db'
PATH_EXCH_INFO = 'Exchange_info/'
PATH_FOLDER_KEY = '/home/seb/Documents/Crypto'

daily_interest_dict = {
    'BTC':0.00035,
    'ETH':0.000275,
    'XRP':0.0001,
    'BNB':0.003,
    'TRX':0.000225,
    'USDT':0.0003,
    'LINK':0.00025,
    'EOS':0.0002,
    'ADA':0.0002,
    'ONT':0.0001,
    'USDC':0.0003,
    'ETC':0.0002,
    'LTC':0.0002,
    'XLM':0.0001,
    'XMR':0.0002,
    'NEO':0.0001,
    'ATOM':0.0002,
    'DASH':0.0002,
    'ZEC':0.0001,
    'MATIC':0.0002,
    'BUSD':0.000275,
    'BAT':0.000225,
    'IOST':0.0001,
    'VET':0.0001,
    'QTUM':0.0001,
    'IOTA':0.0002,
    'XTZ':0.0001,
    'BCH':0.0002,
    'RVN':0.0001
}

################################################# libraries #################################################

### Usefull lib
import os
import sys
import ccxt
import json
import time
import calendar
from datetime import datetime, date
import sqlite3
import math
from pprint import pprint
from operator import itemgetter

### Stats libs
import pandas as pd
import numpy as np
import scipy.stats  as stats
from pyfinance.ols import PandasRollingOLS
from statsmodels.tsa.stattools import coint

### Graph libs
import matplotlib.pyplot as plt
import seaborn as sns

### My lib
import warnings
from MyError import MyError
from MyError import MyWarning
from MyError import WarningFetch
from MyError import ErrorFetch

sys.path.insert(1, PATH_FOLDER_KEY)
from key import key

# TODO: create a unique warning for fetching functions


################################################### class ###################################################
class DataBase():
    def __init__(self):
        self.conn = sqlite3.connect(PATH_DATABASE)

class Trade:
    #TODO: Simu_mode:
    #   - Add multiple Trade object
    # TODO: Add limit order options (add control over slippage)
    #           => Add control over slippage
    #           => Less slippage cost
    # TODO: get current price with order/book bid and ask
    # TODO: slippage estimation
    # TODO: Add exit case on margin account when short looses
    # TODO: Add min profit entry

    def __init__(self,df_hist,base1,base2,quote ='USDT'):
        binance_info = read_json('Exchange_info/binance.json')
        self.quote = quote
        symbol1 = base1 +'/'+ quote
        symbol2 = base2 + '/' + quote

        self.openedPosition = False
        self.long = ''
        self.short = ''
        self.long_entry = 0
        self.short_entry = 0
        # self.shortfees
        self.long_amount = 0
        self.borrow = {base1: [], base2: []}  # [t_stamp_s,amount]

        # precision = binance_info[symbol]['precision']['amount']


        #My attributes
        self.simu_mode=1
        self.simu_balance_spot={quote:105,base1:0,base2:0}
        self.simu_balance_margin = {quote:35,base1:0,base2:0}


        self.df_hist = df_hist

        self.min_amounts = [binance_info[symbol1]['limits']['amount']['min'],binance_info[symbol2]['limits']['amount']['min']]
        self.min_costs = [binance_info[symbol1]['limits']['cost']['min'] * 1.5, binance_info[symbol2]['limits']['cost']['min'] * 1.5]
        self.fee_rates = {base1:binance_info[symbol1]['taker'],base2:binance_info[symbol2]['taker']}
        self.precisions = {base1:binance_info[symbol1]['precision']['amount'],base2:binance_info[symbol2]['precision']['amount']}
        self.intr_h_rates = {base1:daily_interest_dict[base1]/24,base2:daily_interest_dict[base2]/24}

    def simu_get_profit_loss(self, long_price_c:float, short_price_c:float,t_stamp_s):

        interest_rate = self.intr_h_rates[self.short]
        borrow_duration_h = (t_stamp_s - self.borrow[self.short][0]) // 3600 + 1
        interest_rate_duration = round_up(interest_rate * borrow_duration_h, 8)

        if self.long_entry != 0:
            profit_loss = long_price_c / self.long_entry \
                            + self.short_entry / short_price_c \
                            - 2 * self.fee_rates[self.short] \
                            - 2 * self.fee_rates[self.long] \
                            - interest_rate_duration

        else:
            raise ErrorTrade('no position opened')
            # upgrade with short fees
            # for now 1.996 is the null profit_loss
        return profit_loss

    def get_balance_margin(self):
        if self.simu_mode:
            return self.simu_balance_margin
        else:
            balance_margin_btc = float(binance.sapi_get_margin_account()['totalAssetOfBtc'])
            balance_margin_usdt = balance_margin_btc * binance.fetch_ticker('BTC/USDT')['close']
            return balance_margin_usdt

    def get_balance_spot(self):
        if self.simu_mode:
            return self.simu_balance_spot
        else:
            balance_list = binance.fetch_balance()['free']
            tot_value_usdt = 0
            for asset, value in balance_list.items():
                if value != 0:
                    if asset == 'USDT':
                        tot_value_usdt += value
                    else:
                        tot_value_usdt += value*binance.fetch_ticker(asset+'/USDT')['close']
            return tot_value_usdt

    def get_balance_tot(self):
        if self.simu_mode:
            return self.simu_balance_spot[self.quote] + self.simu_balance_margin[self.quote]
        else:
            balance_tot_btc = self.get_balance_spot()+self.get_balance_margin()
            return balance_tot_btc

    def simu_get_pos_sizes(self, long_price, base_long, short_price, base_short):
        balance = self.get_balance_spot()[self.quote]
        long_amount = round_down(balance*(0.15/long_price),self.precisions[base_long]) #default 0.13
        short_amount = round_down(balance*(0.15/short_price),self.precisions[base_short])

        long_cost = long_amount * long_price
        short_cost = short_amount * short_price
        return {'amounts':{'long':long_amount,'short':short_amount},
                'costs':{'long':long_cost,'short': short_cost}}

    def simu_check_size(self, pairs, pos_sizes):
        if((self.min_amounts[0] <= pos_sizes['amounts']['long']) or (self.min_amounts[1] <= pos_sizes['amounts']['short'])):
            if ((self.min_costs[0] <= pos_sizes['costs']['long'] or self.min_costs[1] <= pos_sizes['costs']['short'])):
                return True
        return False

    def simu_market_long_buy(self,t_stamp_s,amount:float, base:str, quote:str='USDT')->dict:

        price = self.df_hist.loc[t_stamp_s,base].item()
        buy_quote = price*amount
        fee_base = self.fee_rates[base]*amount
        fee_quote = price*fee_base
        self.simu_balance_spot[quote] -= buy_quote
        self.simu_balance_spot[base] += amount - fee_base

        self.long_amount += amount - fee_base

        informations = {
            'buy_base': amount,
            'buy_quote': buy_quote,
            'price': price,
            'fee_base': fee_base,
            'fee_quote': fee_quote,
            't_stamp_s':t_stamp_s
        }
        return informations

    def simu_market_long_sell(self, t_stamp_s, amount: float, base: str, quote: str = 'USDT') -> dict:

        price = self.df_hist.loc[t_stamp_s, base].item()
        sell_quote = price * amount
        fee_quote = self.fee_rates[base]*sell_quote
        fee_base = self.fee_rates[base]*amount

        self.simu_balance_spot[quote] += sell_quote - fee_quote
        self.simu_balance_spot[base] -= amount

        self.long_amount -= amount

        informations = {
            'sell_base': amount,
            'sell_quote': sell_quote,
            'price': price,
            'fee_base': fee_base,
            'fee_quote': fee_quote,
            't_stamp_s': t_stamp_s
        }
        return informations

    def simu_market_short_sell(self, t_stamp_s, amount: float, base: str, quote: str = 'USDT') -> dict:

        price = self.df_hist.loc[t_stamp_s, base].item()
        sell_quote = price * amount
        fee_quote = self.fee_rates[base] * sell_quote
        fee_base = self.fee_rates[base] * amount

        self.borrow[base].append(t_stamp_s)
        self.borrow[base].append(amount)
        self.simu_balance_margin[quote] += sell_quote - fee_quote
        self.simu_balance_margin[base] -= amount



        informations = {
            'sell_base': amount,
            'sell_quote': sell_quote,
            'price': price,
            'fee_base': fee_base,
            'fee_quote': fee_quote,
            't_stamp_s': t_stamp_s
        }
        return informations

    def simu_market_short_buy(self, t_stamp_s, amount: float, base: str, quote: str = 'USDT') -> dict:

        price = self.df_hist.loc[t_stamp_s, base].item()
        buy_quote = price * amount
        fee_base = self.fee_rates[base] * amount
        fee_quote = price * fee_base

        debt = self.borrow[self.short][1]
        interest_rate = self.intr_h_rates[self.short]
        borrow_duration_h = (t_stamp_s - self.borrow[self.short][0]) // 3600 + 1
        interest = amount * interest_rate * borrow_duration_h

        if amount - fee_base >= debt + interest:
            self.borrow[base] = []
        else:
            warnings.warn('Error simu repay',WarningTrade)
            self.borrow[base] = []

        self.simu_balance_margin[quote] -= buy_quote
        self.simu_balance_margin[base] += (amount - fee_base - interest)



        informations = {
            'buy_base': amount,
            'buy_quote': buy_quote,
            'price': price,
            'fee_base': fee_base,
            'fee_quote': fee_quote,
            'interest':interest,
            't_stamp_s': t_stamp_s
        }
        return informations

    def simu_close_spread(self,t_stamp_s):
        long_amount  = round_down(self.long_amount,self.precisions[self.long])
        self.simu_market_long_sell(t_stamp_s,long_amount,self.long)

        repay_amount = 0
        if(self.short_entry)>0:
            debt = self.borrow[self.short][1]
            interest_rate = self.intr_h_rates[self.short]
            borrow_duration_h = (t_stamp_s - self.borrow[self.short][0]) // 3600 + 1
            interest =  round_up(debt * interest_rate * borrow_duration_h,8)
            debt_tot = debt + interest

            # Must take into account final fee transaction :
            fee_rate = self.fee_rates[self.short]
            amount_to_add = (fee_rate / (1 - fee_rate)) * debt_tot
            repay_amount = round_up(debt_tot + amount_to_add, self.precisions[self.short])

        else:
            raise ErrorTrade('close spread asked wihtout opened positions')

        self.simu_market_short_buy(t_stamp_s,repay_amount,self.short)

    def reset(self):
        self.openedPosition = False
        self.short = ''
        self.long = ''
        self.short_entry = 0
        self.long_entry = 0
        self.long_amount = 0





    def get_pos_sizes(self, long_price, short_price):
        balance = self.get_balance_spot()
        self.long_amount = balance*(0.15/long_price) #default 0.13
        self.short_amount = balance*(0.15/short_price)
        return [self.long_amount,self.short_amount]

    def check_size(self, pairs, pos_sizes):
        details = read_json('Exchange_info/binance.json')
        min_sizes = []
        for pair in pairs:
            if pair in details.keys():
                min_sizes.append(details[pair]['limits']['amount']['min'])
            else:
                print(pair, 'not in exchange list')
        print('min sizes:',min_sizes)


        if(self.min_sizes[0]<=pos_sizes[0] and min_sizes[1]<=pos_sizes[1]):
            return True
        return False

    # no checks for decimal or amount min or transaction fees, should be done before function call -> impact on amount round up or down
    def market_long_buy(self,amount:float, base:str, quote:str='USDT')->dict:
        """
        Place a market buy order in the sport account.

        :param amount: Amount wanted in base currency
        :param base: Base currency
        :param quote: Quote currency
        :return: Trade informations
        """
        # TODO: add .has['createMarketOrder']
        symbol = base+'/'+quote

        # Buy
        buy_info = binance.create_market_buy_order(symbol, amount)
        json_file('test.json',buy_info)
        # Buy checks
        if buy_info['symbol'] != symbol or buy_info['status'] != 'closed':
            raise ErrorTrade('buy check failed')

        buy_amount = buy_info['amount']
        if buy_amount != amount:
            amount_error = buy_amount - amount
            raise ErrorTrade('Bought amount is: %s than amount wanted'%(amount_error))

        informations = {
            'buy_base': buy_amount,
            'buy_quote': float(buy_info['info']['cummulativeQuoteQty']),
            'price': buy_info['average'],
            'fee_base': buy_info['fee']['cost'],
            'fee_quote': buy_info['fee']['cost']*buy_info['average'],
            'buy_id': buy_info['info']['orderId']
        }
        return informations

    def market_long_sell(self,amount:float, base:str, quote:str='USDT')->dict:
        """
        Place a market sell order in the sport account.

        :param amount: Amount wanted in base currency
        :param base: Base currency
        :param quote: Quote currency
        :return: Trade informations
        """
        #TODO: add .has['createMarketOrder']
        symbol = base+'/'+quote

        # Sell
        sell_info = binance.create_market_sell_order(symbol, amount)
        json_file('test.json',sell_info)
        # sell checks
        if sell_info['symbol'] != symbol or sell_info['status'] != 'closed':
            raise ErrorTrade('sell check failed')

        sell_amount = sell_info['amount']
        if sell_amount != amount:
            amount_error = sell_amount - amount
            raise ErrorTrade('Sold amount is: %s than amount wanted'%(amount_error))

        informations = {
            'sell_base': sell_amount,
            'sell_quote': float(sell_info['info']['cummulativeQuoteQty']),
            'fee_base': sell_info['fee']['cost'],
            'price': sell_info['average'],
            'fee_quote': sell_info['fee']['cost']*sell_info['average'],
            'sell_id': sell_info['info']['orderId']
        }
        return informations

        return binance.create_market_sell_order(symbol, amount)

    # https://github.com/binance-exchange/binance-official-api-docs/blob/master/margin-api.md
    def market_short_sell(self, amount:float, base:str, quote:str='USDT')->dict:
        """
        Place a market short sell order in the margin account.
        :param base: Base currency
        :param quote: Quote currency
        :return: Trade informations
        """
        #FIRST: BORROW ASSET
        loan_id = binance.sapi_post_margin_loan({'asset': base, 'amount': amount})['tranId']

        #Wait broker processing the borrow
        loan_info = {}
        nb_tries = 5
        for n_try in range(0,nb_tries):
            loan_info = binance.sapi_get_margin_loan({'asset': base, 'txId': loan_id})
            if int(loan_info['total']) == 1:
                break
            if (n_try >=nb_tries-1):
                raise ErrorTrade('Number of tries reached, problem in fetching loan infos')
            time.sleep(binance.rateLimit/1000)

        #Check loan status
        loan_asset = loan_info['rows'][0]['asset']
        loan_status = loan_info['rows'][0]['status']
        if loan_asset != quote or loan_status != 'CONFIRMED':
            ErrorTrade('Error loan broker')

        # Check that amount borrowed matches the amount aked
        loan_amount=float(loan_info['rows'][0]['principal'])
        if loan_amount != amount:
            amount_error = loan_amount - amount
            warnings.warn('Amount loaned is: %s than amount wanted'%(amount_error),WarningTrade)

        #SECOND: SELL ASSET
        sell_info = binance.sapi_post_margin_order({'symbol':base+quote,'side':'SELL','type':'MARKET','quantity':loan_amount})

        #Check sell
        if sell_info['symbol'] != base + quote or sell_info['status'] != 'FILLED':
            raise ErrorTrade('sell check failed')
        sell_amount = float(sell_info['executedQty'])
        if sell_amount != amount:
            amount_error = sell_amount - amount
            raise ErrorTrade('Bought amount is: %s than amount wanted'%(amount_error))

        #Fees calculous
        fee_quote = 0
        for i in sell_info['fills']:
            if i['commissionAsset'] != quote:
                raise ErrorTrade('quote is different from comissionAsset')
            fee_quote += float(i['commission'])

        informations = {
            'loan_base': loan_amount,
            'loan_id': loan_id,
            'sell_base': sell_amount,
            'sell_quote': float(sell_info['cummulativeQuoteQty']),
            'fee_quote': fee_quote,
            'sell_id': sell_info['orderId']
        }
        return informations

    def market_short_buy(self,amount:float, base:str, quote:str='USDT')->dict:
        """
        Place a market short buy order in the margin account.
        :param base: Base currency
        :param quote: Quote currency
        :return: Trade informations
        """
        # FIRST: BUY ASSET (+COMMISSION)
        buy_info = binance.sapi_post_margin_order({'symbol': base + quote, 'side': 'BUY', 'type': 'MARKET', 'quantity': amount})

        # Buy checks
        if buy_info['symbol'] != base + quote or buy_info['status'] != 'FILLED':
            raise ErrorTrade('buy check failed')
        buy_amount = float(buy_info['executedQty'])
        if buy_amount != amount:
            amount_error = buy_amount - amount
            raise ErrorTrade('Bought amount is: %s than amount wanted'%(amount_error))

        #Fees calculous
        fee_quote = 0
        fee_base = 0
        for i in buy_info['fills']:
            if i['commissionAsset'] != base:
                raise ErrorTrade('base is different from comissionAsset')
            fee_quote += float(i['commission'])*float(i['price'])
            fee_base += float(i['commission'])


        # SECOND: REPAY ASSET + INTERESTS
        repay_id = binance.sapi_post_margin_repay({'asset': base , 'amount': amount})['tranId']

        # Wait broker processing the repay
        repay_info = {}
        nb_tries = 5
        for n_try in range(0,nb_tries):
            repay_info = binance.sapi_get_margin_repay({'asset': base, 'txId': repay_id})
            if int(repay_info['total']) == 1:
                break
            if (n_try >=nb_tries-1):
                raise ErrorTrade('Number of tries reached, problem in fetching repay infos')
            time.sleep(binance.rateLimit/1000)

        # Check repaid status
        repay_asset = repay_info['rows'][0]['asset']
        repay_status = repay_info['rows'][0]['status']
        if repay_asset != quote or repay_status != 'CONFIRMED':
            ErrorTrade('Error repay broker')

        # Check that the amount repaid matches the amount aked
        repay_amount= float(repay_info['rows'][0]['amount'])
        debt = float(repay_info['rows'][0]['principal'])
        interest = float(repay_info['rows'][0]['interest'])

        if repay_amount != debt+interest:
            amount_error =  repay_amount - amount
            ErrorTrade('Amount repayed is: %s than debt + interest'%(amount_error))

        informations = {
            'buy_base': buy_amount,
            'buy_quote': float(buy_info['cummulativeQuoteQty']),
            'fee_base': fee_base,
            'fee_quote': fee_quote,
            'buy_id': buy_info['orderId'],
            'repay_base': repay_amount,
            'debt': debt,
            'interest': interest,
            'repay_id': repay_id
        }
        return informations


############################################### main functions ###############################################
def run():
    # Init
    t0 = time.time()

    db = DataBase()
    conn = DataBase().conn
    cursor = conn.cursor()

    exch_list = ['binance', 'bitfinex', 'kraken']
    exch = {}
    for exch_id in exch_list:
        exchange_class = getattr(ccxt, exch_id)
        exch[exch_id] = exchange_class(key[exch_id])

    binance = exch['binance']

    check_update_exch_info(exch, 1)

    ################# PANDAS COINTEGRATION ##################
    # new_date = int(tstamp_s_localdate('2020-04-21 00:00:00')*1000)
    # old_date = int(tstamp_s_localdate('2020-04-20 00:00:00')*1000)
    #
    # base_list = ['ETH','NEO','QTUM','EOS','TRX','BNB','DOGE','XMR','DASH','LTC']
    # quote = 'USDT'
    #
    # get_all_historical(exch,conn)
    #
    # for i in base_list:
    #     get_ohlcv_db_v2(exch['binance'], 'ETH' + '/' + quote, '1m',time.time(),'2020-04-24 00:00:00',cursor)
    #
    # pair_list = []
    # for i in base_list:
    #     pair_list.append(i+'/'+quote)
    # dict_corr = {}
    # for i in base_list:
    #     table_name = get_table_name(exch['binance'], i + '/' + quote, '1m')
    #     dict_corr[i] = pd.Series(select_data_tstamp(table_name, new_date, old_date, cursor))
            #select_data_tstamp has changed
    #     print(len(dict_corr[i]))
    #     print(((new_date - old_date) // convert_to_ms('1h')) + 1)
    # df = pd.DataFrame(dict_corr)
    #
    # for i in list(df):
    #     for j in list(df):
    #         if i != j:
    #             pair = i + '-' + j
    #             coint_result = coint(df[i], df[j])
    #             print(pair, coint_result)

        # ETH - TRX(-0.6488947656936879, 0.951877167975573, array([-3.92061856, -3.34956216, -3.05376396]))
        # refer to https://www.statsmodels.org/stable/generated/statsmodels.tsa.stattools.coint.html

    ################## PANDAS CORR ##################

    # base_list = ['ETH','TRX','BNB','DOGE','XMR','DASH','LTC']
    # base_list = ['ETH', 'TRX']
    # quote = 'USDT'
    #
    # pair_list = []
    # for i in base_list:
    #     pair_list.append(i+'/'+quote)
    #
    # new_date = '2020-04-20 00:00:00'
    # old_date = '2020-04-28 00:00:00'
    #
    # dict_corr = {}
    # for i in base_list:
    #     data = get_ohlcv(binance, i + '/' + quote, '1m', new_date, old_date, cursor)
    #     conn.commit()
    #     data_close = []
    #     data_tstamp = []
    #     for j in data:
    #         # data_tstamp.append(datetime.fromtimestamp(j[0]/1000))
    #         data_tstamp.append(int(j[0] / 1000))
    #         data_close.append(j[4])
    #     # dict_corr[i] = pd.Series(data_close,index=pd.DatetimeIndex(data_tstamp))
    #     dict_corr[i] = pd.Series(data_close, index=data_tstamp)
    # df_corr = pd.DataFrame(dict_corr)
    # corr_matrix = df_corr.diff().corr()
    # print(corr_matrix)
    #
    # print(stats.pearsonr(df_corr.diff()['ETH'], df_corr.diff()['TRX']))
    #
    # conn.commit()  # Save arbot database changes
    # conn.close()  # Closes arbot database
    #
    # print(MyWarning.number_warning, 'warnings')
    # print('Time prog secs', '%.2f' % (time.time() - t0))

def trading():
    '''Statistical arbitrage algorithm'''

    # Init
    t0 = time.time()
    db = DataBase()
    conn = DataBase().conn
    cursor = conn.cursor()

    #Multi exchange possibility
    exch_list = ['binance', 'bitfinex', 'kraken']
    exch = {}
    for exch_id in exch_list:
        exchange_class = getattr(ccxt, exch_id)
        exch[exch_id] = exchange_class(key[exch_id])

    binance = exch['binance']

   ################ CORR MATRIX ################

    quote = 'USDT'
    t_frame = '1m'
    margin_assets = ['BTC', 'ETH', 'XRP', 'BNB', 'TRX', 'LINK', 'EOS', 'ADA', 'ONT','USDC', 'ETC', 'LTC',
                     'XLM', 'XMR', 'NEO', 'ATOM', 'DASH', 'ZEC', 'MATIC', 'BUSD', 'BAT', 'IOST', 'VET', 'QTUM',
                     'IOTA', 'XTZ', 'BCH', 'RVN']

    #Time window of the simulation
    old_date = '2020-04-20 00:00:00'
    new_date = '2020-04-28 00:00:00'

    #Correlation calculation on all binance margin assets
    dict_corr = {}
    for i in margin_assets:
        data = get_ohlcv(binance, i + '/' + quote, t_frame, old_date, new_date, cursor)
        conn.commit()
        data_close = []
        data_tstamp = []
        for j in data:
            # data_tstamp.append(datetime.fromtimestamp(j[0]/1000))
            data_tstamp.append(int(j[0] / 1000))
            data_close.append(j[4])
        # dict_corr[i] = pd.Series(data_close,index=pd.DatetimeIndex(data_tstamp))
        dict_corr[i] = pd.Series(data_close, index=data_tstamp)

    df_coint = pd.DataFrame(dict_corr)

    corr_matrix = df_coint.diff().corr()
    pd.set_option("display.max_rows", None, "display.max_columns", None)

    ##### Corr matrix heatmap #####
    # # Generate a mask for the upper triangle
    # mask = np.triu(np.ones_like(corr_matrix, dtype=np.bool))
    # # Set up the matplotlib figure
    # f, ax = plt.subplots(figsize=(11, 9))
    # sns.heatmap(corr_matrix)
    # plt.show()

    # print(corr_matrix)
    #Displays only the top correlation in the selected time window
    print(get_top_correlations(corr_matrix,20))

    ######### COINT - NOT USED #########

    # coint_matrix = []
    # n = 0
    # for i in range (0,len(list(df_coint))):
    #     for j in range (i+1,len(list(df_coint))):
    #         pair = list(df_coint)[i] + '-' + list(df_coint)[j]
    #         coint_result = list(coint(df_coint[list(df_coint)[i]], df_coint[list(df_coint)[j]]))
    #         coint_result.append(pair)
    #         print(coint_result)
    #         coint_matrix.append(coint_result)
    #         n+=1
    #
    # pprint(sorted(coint_matrix,key=itemgetter(1)))
    # print(n)
    # ETH - TRX(-0.6488947656936879, 0.951877167975573, array([-3.92061856, -3.34956216, -3.05376396]))
    # refer to https://www.statsmodels.org/stable/generated/statsmodels.tsa.stattools.coint.html

    #Pair list for the simulationspread_graph
    base_list = ['EOS', 'LTC']
    quote = 'USDT'

    pair_list = []
    for i in base_list:
        pair_list.append(i + '/' + quote)


    #Begining date for calculations
    old_calc = '2020-04-13 00:00:00'

    dict_corr = {}
    for i in base_list:
        data = get_ohlcv(binance, i + '/' + quote, '1m', old_calc, new_date, cursor)
        conn.commit()
        data_close = []
        data_tstamp = []
        for j in data:
            # data_tstamp.append(datetime.fromtimestamp(j[0]/1000))
            data_tstamp.append(int(j[0] / 1000))
            data_close.append(j[4])
        # dict_corr[i] = pd.Series(data_close,index=pd.DatetimeIndex(data_tstamp))

        dict_corr[i] = pd.Series(data_close, index=data_tstamp)
    df_all_data = pd.DataFrame(dict_corr)


    #Generation time list
    old_tstamp_s = int(tstamp_s_localdate(old_date))
    new_tstamp_s = int(tstamp_s_localdate(new_date))
    tstamp_list = list(df_all_data.loc[old_tstamp_s:new_tstamp_s:20].index.values)

    #Init monitoring parmaters
    result = {
        'tstamp_s': [],
        'short': [],
        'long': [],
        'profit': [],
        'balance': [],
        'status': []
    }
    n = 0
    current_tstamp_s_list = []
    spread_list =[]
    zscore_res_list =[]
    gain_list = []

    #Set trading params
    base1 = pair1 = base_list[0]
    base2 = pair2 = base_list[1]

    trade = Trade(df_all_data, base1, base2)
    trade.simu_mode = 1
    balance_before = trade.get_balance_tot()

    #Set spread and zscore calculation window
    w_s = 500
    w_z = 2000

    print('Start simulation')
    for current_tstamp_s in tstamp_list:

        #Caculation data frome dataframe
        df = df_all_data.loc[current_tstamp_s-60*3*w_z:current_tstamp_s]

        # Spread and zscore calculous
        spread = get_spread(df, w=w_s)
        zscore = get_zscore(spread, w=w_z)

        #get curent zscore (-1 min )
        zscore_res = zscore[base1+'-'+base2].iat[-1]

        #Option: data storage for ploting spread and zscore
        zscore_res_list.append(zscore_res)
        current_tstamp_s_list.append(current_tstamp_s)
        spread_list.append(spread[base1+'-'+base2].iat[-1])

        #Algo:
        if not trade.openedPosition:
            pair1_c = df.loc[current_tstamp_s, base1].item()
            pair2_c = df.loc[current_tstamp_s, base2].item()
            if zscore_res > 3:
                pos_sizes = trade.simu_get_pos_sizes(pair2_c, pair2, pair1_c, pair1)
                if trade.simu_check_size([pair2, pair1], pos_sizes):
                    short = trade.simu_market_short_sell(current_tstamp_s, pos_sizes['amounts']['short'], pair1)
                    long = trade.simu_market_long_buy(current_tstamp_s, pos_sizes['amounts']['long'], pair2)
                    trade.openedPosition = True
                    trade.short = pair1
                    trade.long = pair2
                    trade.short_entry = pair1_c
                    trade.long_entry = pair2_c

            if zscore_res < -3:
                pos_sizes = trade.simu_get_pos_sizes(pair1_c,pair1, pair2_c,pair2)
                if trade.simu_check_size([pair1, pair2], pos_sizes):
                    short = trade.simu_market_short_sell(current_tstamp_s, pos_sizes['amounts']['short'], pair2)
                    long = trade.simu_market_long_buy(current_tstamp_s, pos_sizes['amounts']['long'], pair1)
                    trade.openedPosition = True
                    trade.short = pair2
                    trade.long = pair1
                    trade.short_entry = pair2_c
                    trade.long_entry = pair1_c


        else:
            if trade.long == pair2 and trade.short == pair1:
                profit_loss = trade.simu_get_profit_loss(pair2_c,pair1_c,current_tstamp_s)
                if zscore_res > -0.1 and zscore_res < 0.1:
                    trade.simu_close_spread(current_tstamp_s)
                    trade.reset()

                    result['tstamp_s'].append(current_tstamp_s)
                    result['short'].append(pair1)
                    result['long'].append(pair2)
                    result['profit'].append(profit_loss)
                    result['balance'].append(trade.get_balance_tot())
                    result['status'].append('win')

                elif profit_loss < -0.15:
                    trade.simu_close_spread(current_tstamp_s)
                    trade.reset()

                    result['tstamp_s'].append(current_tstamp_s)
                    result['short'].append(pair1)
                    result['long'].append(pair2)
                    result['profit'].append(profit_loss)
                    result['balance'].append(trade.get_balance_tot())
                    result['status'].append('loose')

            if trade.long == pair1 and trade.short == pair2:
                profit_loss = trade.simu_get_profit_loss(pair1_c,pair2_c,current_tstamp_s)
                if zscore_res > -0.1 and zscore_res < 0.1:
                    trade.simu_close_spread(current_tstamp_s)
                    trade.reset()

                    result['tstamp_s'].append(current_tstamp_s)
                    result['short'].append(pair2)
                    result['long'].append(pair1)
                    result['profit'].append(profit_loss)
                    result['balance'].append(trade.get_balance_tot())
                    result['status'].append('win')

                elif profit_loss < -0.15:
                    trade.simu_close_spread(current_tstamp_s)
                    trade.reset()

                    result['tstamp_s'].append(current_tstamp_s)
                    result['short'].append(pair2)
                    result['long'].append(pair1)
                    result['profit'].append(profit_loss)
                    result['balance'].append(trade.get_balance_tot())
                    result['status'].append('loose')

        # Completion report:
        if n%100 == 0:
            print(n,'/',len(tstamp_list))
            t1 = time.time()
        if n%100 == 99:
            print('Time loop secs', '%.2f' % (time.time() - t1))
        n += 1

    #Store results in csv file
    buffer_dict = {}
    for k,val in result.items():
        buffer_dict[k]  = pd.Series(val)
    df_result = pd.DataFrame(buffer_dict)
    df_result.set_index('tstamp_s')
    df_result.to_csv(r'test.csv', index=False, header=True)

    #Benefit calc
    balance_after = trade.get_balance_tot()
    gain = balance_after - balance_before
    gain_list.append([gain,w_s,w_z])
    pprint(sorted(gain_list, key=itemgetter(0), reverse=True))

    #Ploting spread and zcore
    zscore_graph = pd.Series(zscore_res_list, index=pd.to_datetime(current_tstamp_s_list, unit='s'))
    spread_graph = pd.Series(spread_list, index=pd.to_datetime(current_tstamp_s_list, unit='s'))

    plt.figure(1)
    plt1 = zscore_graph.plot(title = 'zscore')

    plt.figure(2)
    plt2 = spread_graph.plot(title='spread')
    plt.show()




    conn.close()  # Closes arbot database
    print(MyWarning.number_warning, 'warnings')
    print('Time prog secs', '%.2f' % (time.time() - t0))


################################################## funtctions ################################################

########## TRADING STATS ##########

def get_spread(data: object, w: int = 30):
    """
    Get the spread between two data sets with a linear regression.


    :param data: Pandas data frame
    :param w: Width used for the Rolling Ordinary Least Square regression (aka. Linear Regression)
    :return: Spread data list
    """
    spread = pd.DataFrame()
    pair1 = list(data)[0]
    pair2 = list(data)[1]
    pairs = pair1 + '-' + pair2

    # Find the best linear function between data one et two in a w section
    rolling_ols = PandasRollingOLS(y=data[pair2], x=data[pair1], window=w)

    # Use the linear coefficients to determine the spread
    spread[pairs] = data[pair1] - rolling_ols.beta['feature1'] * data[pair2]

    return spread

def get_zscore(spread, w=30):
    """
    Calculous of the standard score. https://en.wikipedia.org/wiki/Standard_score

    :param spread: Spread data list
    :param w: data with of a zscore
    :return: Standard core of the spread
    """

    # Standart deviation
    std = spread.rolling(center=False, window=w).std()

    # Mean
    mean = spread.rolling(center=False, window=w).mean()

    # current spread value, window = 1
    x = spread.rolling(center=False, window=1).mean()

    # Standard core  equation: normalisation of the spread
    zscore = (x - mean) / std

    # Drops null values
    zscore.dropna(inplace=True)

    return zscore


########## UTILITIES ##########
def tstamp_s_localdate(localdate: str) -> int:
    """
    Transforms a local date into timestamp in secs.

    :param localdate: format '2020-04-24 18:59:59'
    :return: timestamp in secs
    """
    return time.mktime(datetime.strptime(localdate, '%Y-%m-%d %H:%M:%S').timetuple())

def round_up(n: float, decimals: int = 0):
    multiplier = 10 ** decimals
    return math.ceil(n * multiplier) / multiplier

def round_down(n: float, decimals: int = 0):
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier

def list_meth(obj: object):
    """Gives a list of all method of the object"""
    for i in dir(obj):
        print(i)

def list_attr(obj: object):
    """Gives a list of all attributes of the object"""
    for key, value in obj.__dict__.items():
        print(key, ":", value)

def json_file(filename: str, data: list):
    """Converts a dictionary into a json file"""
    filename_ext = filename
    with open(filename_ext, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=1)

def read_json(filename: str) -> list:
    """Read a json file and return it as a list or dictionary"""
    filename_ext = filename
    with open(filename_ext, 'r') as f:
        dict = json.load(f)
    return dict

def get_redundant_pairs(df):
    '''Get diagonal and lower triangular pairs of correlation matrix'''
    pairs_to_drop = set()
    cols = df.columns
    for i in range(0, df.shape[1]):
        for j in range(0, i+1):
            pairs_to_drop.add((cols[i], cols[j]))
    return pairs_to_drop

def get_top_correlations(df, n=5):
    au_corr = df.corr().unstack()
    labels_to_drop = get_redundant_pairs(df)
    au_corr = au_corr.drop(labels=labels_to_drop).sort_values(ascending=False)
    return au_corr[0:n]



########## EXCHANGE FETCHING ##########
def get_all_historical(exch: list, conn: object):
    """
    !!! BUILD IN PROGRESS !!!
    Fetch all historical from exchange list and store it in the data base.
    Historical properties are set inside the function with t_frame_list.
    :param exch: exchange list
    :param conn: connection database object
    """
    cursor = conn.cursor()
    t_frame_list = [['1m', '2020-04-15T00:00:00Z'], ['5m', '2020-03-21T00:00:00Z']]
    # oldest_date = ['2020-04-19T00:00:00Z','2020-03-21T00:00:00Z']
    # t_frame_list.reverse()
    # oldest_date.reverse()

    print(t_frame_list)

    info_binance = read_json('Exchange_info/binance.json')
    binance_symbol = []

    # for symbol, j in info_binance.items():
    #     table_name = get_table_name(exch['binance'],symbol,'3d')
    #     drop_table_db(table_name,cursor)

    time_matrix = {}
    for t_frame in t_frame_list:
        tloop = time.time()
        nb_pair = 0
        nb_t_frame = 0
        for symbol in info_binance.keys():
            get_ohlcv_db_v2(exch['binance'], symbol, t_frame[0], time.time(), t_frame[1], cursor)
            conn.commit()
            nb_pair += 1
            print('Pairs:', nb_pair, '/', len(info_binance))
            print('Time:', time.time() - tloop)
        print('timeframes:', nb_t_frame, '/', len(time_matrix))
        time_matrix[t_frame[0]] = time.time() - tloop

        for i in time_matrix:
            print(i)

def get_ohlcv(exchange: object, symbol: str, t_frame: str, old_date: str, new_date: str, cursor: object):
    """
    Returns OHLCV data from an old date to a new date.
    Gets the data from the database, fetches missing data from broker and fills the database with it.

    :param exchange: ccxt exchange object
    :param symbol: symbol of the pair to fetch
    :param t_frame: timeframe wanted
    :param old_date: format:'2020-04-01 00:00:00'
    :param new_date: format:'2020-04-01 00:00:00'
    :param cursor: cursor of the database
    :return: data | 0 -> error
    """
    table_name = get_table_name(exchange, symbol, t_frame)
    t_frame_ms = convert_to_ms(t_frame)

    max_data_tstamp_ms = int(tstamp_s_localdate(new_date) * 1000)
    min_data_tstamp_ms = int(tstamp_s_localdate(old_date) * 1000)

    if max_data_tstamp_ms == min_data_tstamp_ms:
        warnings.warn('Need two different dates', WarningFetch)
        return 0

    if (min_data_tstamp_ms % t_frame_ms !=0) or (max_data_tstamp_ms % t_frame_ms != 0):
        warnings.warn('Times asked are not multiple of the the timeframe given', WarningFetch)
        return 0

    # Check if a table has aldready been created and fills it if not
    # Check if that table has aldready been filled and fills it if not
    if not (table_exists_db(table_name, cursor)) or not (data_exists_db(table_name, cursor)):
        data = fetch_ohlcv(exchange, symbol, t_frame, min_data_tstamp_ms, max_data_tstamp_ms)
        if data:
            create_table_db(table_name, cursor)
            # Insert data into db
            insert_pair_db(table_name, data, cursor)
            # print('inside 0')
            return data
        else:
            return 0

    # Get the oldest and newest row timestamp.
    max_db_tstamp_ms = max_column_db(table_name, 'tstamp', cursor)
    min_db_tstamp_ms = min_column_db(table_name, 'tstamp', cursor)

    # Check existing data only with max and min tstamp
    # This works only if there is no holes => more data fetched

    min_data = min_data_tstamp_ms
    max_data = max_data_tstamp_ms
    min_db = min_db_tstamp_ms
    max_db = max_db_tstamp_ms

    if min_data >= max_db:
        # print('inside 1')
        data_fetch = fetch_ohlcv(exchange, symbol, t_frame, max_db, max_data)
        upsert_pair_db(table_name,data_fetch,cursor)

        return select_data_tstamp(table_name,min_data,max_data,cursor)

    elif max_data <= min_db:
        # print('inside 2')
        data_fetch = fetch_ohlcv(exchange, symbol, t_frame, min_data, min_db)
        upsert_pair_db(table_name, data_fetch, cursor)

        return select_data_tstamp(table_name,min_data,max_data,cursor)

    elif min_db < min_data < max_db and max_data > max_db:
        # print('inside 3')
        data_fetch = fetch_ohlcv(exchange, symbol, t_frame, max_db, max_data)
        upsert_pair_db(table_name,data_fetch,cursor)

        return select_data_tstamp(table_name,min_data,max_data,cursor)

    elif min_db < max_data < max_db and min_data < min_db:
        # print('inside 4')
        data_fetch = fetch_ohlcv(exchange, symbol, t_frame, min_data, min_db)
        upsert_pair_db(table_name, data_fetch, cursor)

        return select_data_tstamp(table_name,min_data,max_data,cursor)

    elif max_data >= max_db and min_data <= min_db:
        # print('inside 5')
        if min_data != min_db:
            data_fetch = fetch_ohlcv(exchange, symbol, t_frame, min_data, min_db)
            upsert_pair_db(table_name, data_fetch, cursor)
            json_file('data_fetch1.json',data_fetch)

        if max_data != max_db:
            data_fetch = fetch_ohlcv(exchange, symbol, t_frame, max_db, max_data)
            upsert_pair_db(table_name, data_fetch, cursor)
            json_file('data_fetch2.json', data_fetch)

        return select_data_tstamp(table_name,min_data,max_data,cursor)

    elif max_data <= max_db and min_data >= min_db:
        # print('inside 6')
        return select_data_tstamp(table_name, min_data, max_data,cursor)
    else:
        raise ErrorFetch('Unknown case')

def fetch_ohlcv(exchange: object, symbol: str, t_frame: str, old_ststamp_ms, new_tstamp_ms, limit=1000):
    """
     Fetch OHLCV data from exchange form an old date to a new date.

     :param exchange: ccxt broker object
     :param t_frame: Timeframe wanted
     :param new_tstamp_ms: New date
     :param old_ststamp_ms: Old date
     :param limit: Nb of rows that can be fetch in one request (depends on the broker)
     :return: data as list | 0 -> error
     """
    t_frame_ms = convert_to_ms(t_frame)

    if new_tstamp_ms < old_ststamp_ms:
        warnings.warn('Wrong dates', WarningFetch)
        return 0

    if new_tstamp_ms == old_ststamp_ms:
        warnings.warn('Need two different dates', WarningFetch)
        return 0

    if exchange.has['fetchOHLCV']:
        # check the t_frame parameter in timeframes list of the exchange
        if not (t_frame in exchange.timeframes.keys()):
            warnings.warn('t_frame is not in ' + exchange.id + ' timeframes.', WarningFetch)
            return 0

        # Pagination method to fetch historial OHLCV
        time_loop = old_ststamp_ms
        all_OHLCV = []
        while time_loop <= new_tstamp_ms:
            OHLCV = exchange.fetch_ohlcv(symbol, t_frame, time_loop, limit)
            if len(OHLCV):
                n = 0
                time_loop = OHLCV[len(OHLCV) - 1][0] + t_frame_ms  # widhdraw t_frame
                all_OHLCV += OHLCV
                print('Fetching:', symbol, t_frame, len(all_OHLCV))
            else:
                break
            time.sleep(exchange.rateLimit / 1000)

        total_lenght = ((new_tstamp_ms - old_ststamp_ms) // (t_frame_ms)) + 1
        # q = total_lenght // limit
        # r = total_lenght % limit
        # data = all_OHLCV[0:q * limit + r]

        # Due to exchange server maintenance over the time some data are missin
        # Widthdraw unwanted data from the last fetch of the above loop

        buffer_tstamp = new_tstamp_ms
        found = 0
        i = len(all_OHLCV)-1
        while i >= 0 and found == 0:
            if all_OHLCV[i][0] == buffer_tstamp:
                data = all_OHLCV[0:i+1]
                found = 1
            elif all_OHLCV[i][0] > buffer_tstamp:
                i-=1
            elif all_OHLCV[i][0] < buffer_tstamp:
                data = all_OHLCV[0:i+1]
                found = 1
                warnings.warn('new date not found',WarningFetch)


        json_file('test_data.json', data)

        # Timestamp of the last data must match new timestamp
        if data[-1][0] != (new_tstamp_ms - (new_tstamp_ms % t_frame_ms)):
            warnings.warn('Data missing', WarningFetch)

        # Check good data consistency
        table_name = get_table_name(exchange, symbol, t_frame)

        buffer = []
        for i in range(0, len(data)-1):
            if data[i][0] + t_frame_ms != data[i + 1][0]:
                buffer.append(str(data[i][0]))

        if len(buffer) > 0:
            warnings.warn('broker issue,%s rows are missing check MissingData.json' % (len(buffer)))

        # if missing data, store missing rows tstamp in MissingData log file
        missing_data = read_json('MissingData.json')
        if table_name in missing_data.keys():
            for tstamp in buffer:
                if not (tstamp in missing_data[table_name]):
                    missing_data[table_name].append(tstamp)
            json_file('MissingData.json', missing_data)
        else:
            missing_data.update({table_name: buffer})
            json_file('MissingData.json', missing_data)

        # Data lenght check
        print('data_ohlcv:', len(data))
        print('taill calc:', total_lenght)
        if len(data) == total_lenght:
            return data
        else:
            warnings.warn('data lenght asked does not match the data returned', WarningFetch)
            return data

    else:
        warnings.warn('No fetchOHLCV available for: ' + exchange.id, WarningFetch)
        return 0

def convert_to_ms(t_frame: str) -> int:
    """Converts timeframe into its milliseconds value"""
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

def check_update_exch_info(exch: list, n_days: int = 1):
    """
    Check the json file associated with the exchange informations and update it if nessecary.

    :param exch: list of ccxt exchange objects
    :param n_days: how many days the file can be old
    """
    no_update = 1
    for key, val in exch.items():
        if os.path.isfile('Exchange_info/' + key + '.json'):
            filename = 'Exchange_info/' + key + '.json'
            if time.time() - os.path.getmtime('Exchange_info/' + key + '.json') > n_days * 3600 * 24:
                json_file(filename, val.load_markets())
                no_update = 0
                print('Update of: ' + filename)
        else:
            print('Creation of:' + filename)
            json_file(filename, val.load_markets())
            no_update = 0
    if no_update:
        print('Exchange_info was aldready up to date')


########## DATABASE SQLITE3 ##########
# Warning : Those functions are not proctected against MYSQL injections

# TODO:check that all transactions are stored and up to date

def create_table_db(table_name: str, cursor: object):
    print(table_exists_db(table_name, cursor))
    if not table_exists_db(table_name, cursor):
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

def insert_pair_db(table_name, data: list, cursor: object):
    sql = """INSERT INTO %s(tstamp, open, high, low, close, volume) VALUES(?, ?, ?, ?, ?, ?)""" % (table_name)
    cursor.executemany(sql, data)
    print('Data inserted with success.')

def upsert_pair_db(table_name, data: list, cursor: object):
    sql = """INSERT OR IGNORE INTO %s(tstamp, open, high, low, close, volume) 
            VALUES (?, ?, ?, ?, ?, ?)
             """ % (table_name)

    cursor.executemany(sql, data)
    print('Data inserted with success.')

def delete_all_row_db(table_name: str, cursor: object):
    if data_exists_db(table_name, cursor):
        sql = """DELETE from %s
                      WHERE EXISTS( SELECT *
                                    FROM %s)
                   """ % (table_name, table_name)
        cursor.execute(sql)

def delete_last_6d_row_db(table_name: str, cursor: object):
    limit = time.time() * 1000 - 6 * convert_to_ms('1d')
    if data_exists_db(table_name, cursor):
        sql = """DELETE from %s
               WHERE (tstamp >= %s)
                   """ % (table_name, limit)
        cursor.execute(sql)

def select_data_tstamp(table_name: str, tstamp_ms_old: int, tstamp_ms_new: int, cursor: object):
    sql = """SELECT tstamp,open,high,low,close,volume FROM %s WHERE ( tstamp <= ? AND tstamp >= ?)""" % (table_name)
    rows = cursor.execute(sql, (tstamp_ms_new, tstamp_ms_old))
    data = []
    for row in rows:
        data.append(row)
    print('Getting data from database')
    return data

def select_data_db(table_name: str, cursor: object):
    sql = """SELECT close FROM %s""" % (table_name)
    rows = cursor.execute(sql)
    data = []
    for row in rows:
        data.append(row[0])
    return data

def min_column_db(table_name: str, column: str, cursor: object) -> int:
    sql = """SELECT tstamp FROM %s WHERE tstamp = ( SELECT min(%s) From %s)""" % (table_name, column, table_name)
    rows = cursor.execute(sql)
    data = []
    for row in rows:
        data = row[0]
    return data

def max_column_db(table_name: str, column: str, cursor: object) -> int:
    sql = """SELECT tstamp FROM %s WHERE tstamp = ( SELECT max(%s) From %s)""" % (table_name, column, table_name)
    rows = cursor.execute(sql)
    data = []
    for row in rows:
        data = row[0]
    return data

def table_exists_db(table_name: str, cursor: object):
    sql = """ SELECT count(name) FROM sqlite_master WHERE type='table' AND name='%s' """ % (table_name)
    cursor.execute(sql)
    return cursor.fetchone()[0]

def data_exists_db(table_name: str, cursor: object):
    sql = """SELECT count(*) FROM(SELECT 0 FROM %s LIMIT 1)""" % (table_name)
    cursor.execute(sql)
    return cursor.fetchone()[0]

def drop_table_db(table_name: str, cursor: object):
    cursor.execute("""DROP TABLE %s""" % (table_name))

def get_table_name(exchange: str, symbol: str, t_frame: str) -> str:
    # Get the pair_id from exchange.json in PATH_EXCH_INFO folder
    exchange_info = read_json(PATH_EXCH_INFO + exchange.id + '.json')
    pair_id = exchange_info[symbol]['id']

    # Name table : exchange_symbol_timeframe
    table_name = exchange.id + '_' + pair_id + '_' + t_frame

    return table_name


############################################### main run ###############################################

# run()
trading()

############################################### comments ###############################################
# print(json.dumps(binance_markets, indent=1))
# json_file('json_returns/binance_exchange', binance_exch.load_markets())
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

# old_calc = datetime.fromtimestamp(current_tstamp_s - 7 * 24 * 3600).strftime("%Y-%m-%d %H:%M:%S")
# dt = datetime.strptime('2020-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')
# ts = calendar.timegm(dt.timetuple())
# print(ts)

################# VALIDATION OF get_olhcv_db_v3 #################

# table_name = get_table_name(binance, 'ETH/USDT', '1h')
#     drop_table_db(table_name, cursor)
#
#     new_date = int(tstamp_s_localdate('2020-04-25 00:00:00') * 1000)
#     old_date = binance.parse8601('2020-01-22T00:00:00Z')
#
#     old_date1 = '2020-03-20 00:00:00'
#     new_date1= '2020-03-21 00:00:00'
#
#     test2 = get_ohlcv_db_v3(binance, 'ETH/USDT', '1h', old_date1, new_date1, cursor)
#
#     old_date1 = '2020-03-20 01:00:00'
#     new_date1 = '2020-03-20 23:00:00'
#
#     print('olddate:',int(tstamp_s_localdate(old_date1) * 1000))
#     print('newdate:',int(tstamp_s_localdate(new_date1) * 1000))
#
#     test2 = get_ohlcv_db_v3(binance, 'ETH/USDT', '1h', old_date1, new_date1,cursor)
#     print('lenght:',len(test2))
#     json_file('test2.json',test2)

################# Strat before simu #################
# while true:
#     trade.demo_mode = 1
#     buff_time = exch['binance'].milliseconds()
#     since = buff_time - 9 * 3600 * 1000
#
#     dict_corr = {}
#     for i in pair_list:
#         data = get_ohlcv_v2(exch['binance'], i, '1m', since)
#         print(data)
#         data_close = []
#         for j in data:
#             data_close.append(j[5])
#         dict_corr[i] = pd.Series(data_close)
#         print(len(dict_corr[i]))
#         print(((buff_time - since) // convert_to_ms('1m')) + 1)
#
#     df = pd.DataFrame(dict_corr)
#     spread = get_spread(df)
#     zscore = get_zscore(spread)
#     pair1 = list(df)[0]
#     pair2 = list(df)[1]
#
#     # get current price should be done with an other method than fetch_ohlcv
#     pair1_c = df[pair1].iat[-1]
#     pair2_c = df[pair2].iat[-1]
#     pairs = pair1 + '-' + pair2
#     zscore_res = zscore[pairs].iat[-1]
#
#     if not trade.openedPosition:
#         if zscore_res > 2:
#             short_spread()
#             pos_sizes = trade.get_pos_sizes(pair2_c, pair1_c)
#             if trade.check_size([pair2, pair, ], pos_sizes):
#                 trade.openedPosition = True
#                 trade.short = pair1
#                 trade.long = pair2
#                 trade.short_entry = pair1_c
#                 trade.long_entry = pair2_c
#         if zscore_res < -2:
#             long_spread()
#             pos_sizes = trade.get_pos_sizes(pair1_c, pair2_c)
#             if trade.check_size([pair1, pair2, ], pos_sizes):
#                 trade.openedPosition = True
#                 trade.short = pair2
#                 trade.long = pair1
#                 trade.short_entry = pair2_c
#                 trade.long_entry = pair1_c
#     else:
#         if trade.long == pair2 and trade.short == pair1:
#             profit_loss = trade.get_profit_loss(pair2_c, pair1_c)
#             if zscore_res > -0.1 and zscore_res < 0.1:
#                 close_spread()
#                 reset_prop()
#             elif profit_loss < -0.15:
#                 close_spread()
#                 reset_prop()
#         if trade.long == pair1 and trade.short == pair2:
#             profit_loss = trade.get_profit_loss(pair1_c, pair2_c)
#             if zscore_res > -0.1 and zscore_res < 0.1:
#                 close_spread()
#                 reset_prop()
#             elif profit_loss < -0.15:
#                 close_spread()
#                 reset_prop()

################# Plot tools  #################

# plt1 = df.plot(title='Dataframe')
#
# plt2 = spread.plot(title='Spread')
# plt2.set_xlim(pd.Timestamp('2020-04-27 00:00:00'), pd.Timestamp('2020-04-28 00:00:00'))

# plt3 = zscore.plot(title = 'zscore')

# plt.show()
#
# zsc = []
# col = 0
# w_z= 100

# fig, axes = plt.subplots(nrows=3, ncols=3, sharex=True, figsize=(22, 14))
# for i in range(1,10):
#     row=(i-1)//3
#     if col == 3:
#         col =0
#
#     print(i,'row:',row,'col:',col)
#     zsc.append(get_zscore(get_spread(df, w=10*i), w=w_z))
#     ax = zsc[i-1].plot(title='w_z:20 w_s:%s'%(10*i),ylim=[-3,3],ax=axes[row,col])
#     ax.set_xlim(pd.Timestamp('2020-04-27 00:00:00'), pd.Timestamp('2020-04-28 00:00:00'))
#     ax.legend_.remove()
#     fig.suptitle('ETH-DASH_ZSC_wZ:%s.png'%(w_z),fontsize=16)
#     col += 1
# print('image')
# fig.savefig('ZSCORE/ETH-DASH_ZSC_wZ:%s.png'%(w_z))
# fig.clf()