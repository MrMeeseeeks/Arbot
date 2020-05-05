import ccxt
import json
import time
import math

import warnings
from MyError import WarningTrade
from MyError import ErrorTrade

PATH_FOLDER_KEY = '/home/seb/Documents/Crypto'
sys.path.insert(1, PATH_FOLDER_KEY)
from key import key



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

# Multi exchange possibility
exch_list = ['binance', 'bitfinex', 'kraken']
exch = {}
for exch_id in exch_list:
    exchange_class = getattr(ccxt, exch_id)
    exch[exch_id] = exchange_class(key[exch_id])

binance = exch['binance']

def read_json(filename:str)->list:
    filename_ext = filename
    with open(filename_ext, 'r') as f:
        dict = json.load(f)
    return dict

def json_file(filename:str, data:list):
    filename_ext = filename
    with open(filename_ext, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=1)

def round_up(n: float, decimals: int = 0):
    multiplier = 10 ** decimals
    return math.ceil(n * multiplier) / multiplier

def round_down(n: float, decimals: int = 0):
    multiplier = 10 ** decimals
    return math.floor(n * multiplier) / multiplier

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



################## TEST ORDER FUNCTIONS IN MAIN ##################
# binance_info = read_json('Exchange_info/binance.json')
# base = 'QTUM'
# quote = 'USDT'
# symbol = base + '/' + quote
# precision = binance_info[symbol]['precision']['amount']
# fee = binance_info[symbol]['taker']
# min_amount = binance_info[symbol]['limits']['amount']['min']
# min_cost = binance_info[symbol]['limits']['cost']['min']
#
# price_quote = 15
# print(price_quote - min_cost)
# if (price_quote - min_cost < 0):
#     print('min cost not ok')
# orderbook = binance.fetch_order_book(symbol)
# bid = orderbook['bids'][0][0] if len(orderbook['bids']) > 0 else None
# ask = orderbook['asks'][0][0] if len(orderbook['asks']) > 0 else None
# spread = (ask - bid) if (bid and ask) else None
#
# print({'bid': bid, 'ask': ask, 'spread': spread})
#
# last_price = binance.fetch_ticker(symbol)['close']
# print('last price:', last_price)
# amount = price_quote / last_price
# final_fee = amount * (fee / (1 - fee))
# final_amount = amount + final_fee
#
# print('amount:', final_amount)
# final_amount_roundup = round_up(final_amount, precision)
# final_amount_rounddw = round_down(final_amount, precision)
# print('amount precision:', final_amount_roundup)
# price_calc = final_amount_roundup * last_price
# print('calc price:', price_calc)



################## MARKET LONG ORDERS ##################
# trade_1 = trade.market_long_buy(final_amount_rounddw,base)
# print(trade_1)
# print(trade.market_long_sell(round_down(trade_1['buy_base']-trade_1['fee_base'],precision),base))



################## MARKET SHORT ORDERS ##################
# print(trade.market_short_sell(final_amount_roundup, base))
#
# margin_assets = binance.sapi_get_margin_account()['userAssets']
# repay_amount = 0
# for i in margin_assets:
#     if i['asset'] == base:
#         if float(i['netAsset']) < 0:
#             debt = float(i['borrowed'])
#             interest = float(i['interest'])
#             debt_tot = debt + interest
#             # Must take into account final fees :
#             amount_to_add = (fee / (1 - fee)) * debt_tot
#             repay_amount = round_up(debt_tot + amount_to_add, precision)
#         else:
#             print('nothing to repay')
#
# print('repay_amout:', repay_amount)
#
# print(trade.market_short_buy(repay_amount, base))

################## TEST TOOLS ##################
# json_file('json_returns/sapi_get_margin_loan.json', binance.sapi_get_margin_loan({'asset':base,'txId':7893281452}))
# json_file('json_returns/sapi_get_margin_account.json', binance.sapi_get_margin_account())
# json_file('json_returns/create_market_buy_order.json', binance.create_market_buy_order(symbol,amount))
# json_file('json_returns/create_market_sell_order.json', binance.create_market_sell_order(symbol, 10.19742500))
# json_file('json_returns/sapi_post_margin_order_sell.json', binance.sapi_post_margin_order(
#    {'symbol':base+quote,'side':'SELL','type':'MARKET','quantity':29.837}))
# json_file('json_returns/sapi_post_margin_order_buy.json', binance.sapi_post_margin_order(
#   {'symbol':base+quote,'side':'BUY','type':'MARKET','quantity':final_amount_roundup}))
# json_file('json_returns/sapi_post_margin_repay.json', binance.sapi_post_margin_repay(
#     {'asset': base , 'amount': final_amount_roundup}))
# json_file('json_returns/sapi_get_margin_loan.json',
# binance.sapi_get_margin_loan({'asset': base, 'startTime': int((time.time() - 3600 * 24 * 5) * 1000)}))