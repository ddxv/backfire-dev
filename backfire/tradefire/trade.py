import numpy as np
import gdax
import json
from datetime import datetime, timedelta
import pandas as pd
import mysql_prices as ms
from time import sleep
import logging
import backfire.tradefire.bot_db
from backfire.settings import EmaSettings
# Not in git
import ema_logic

FORMAT = '%(asctime)s: %(name)s:  %(levelname)s:  %(message)s'
logging.basicConfig(format = FORMAT, filename = 'trade.log', level = logging.INFO)
formatter = logging.Formatter(FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not len(logger.handlers):
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)



def initialize_gdax(test_bool):
    gdax_auth = json.load(open('/home/bitnami/auth/gdax'))
    key = gdax_auth['key']
    secret = gdax_auth['secret']
    passphrase = gdax_auth['passphrase']
    if test_bool == True:
        logger.info("Initialize GDAX Sandbox API")
        ac = gdax.AuthenticatedClient(key, secret, passphrase,
                api_url="https://api-public.sandbox.gdax.com")
    if test_bool == False:
        logger.info("Initialize live GDAX API")
        ac = gdax.AuthenticatedClient(key, secret, passphrase)
    return(ac)

#Tool to search list of dictionaries
def my_find(lst, key, value):
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return dic

##Simply returns open orders
#def get_open_orders():
#    my_orders = ac.get_orders()
#    while type(my_orders) is not list:
#        sleep(2)
#        my_orders = ac.get_orders()
#    if len(my_orders[0]) == 0:
#        young_ids = []
#        old_ids = []
#    else:
#        order_df = pd.DataFrame(my_orders[0])
#        open_orders = order_df[order_df['status'] == 'open']
#        old_ids = open_orders[pd.to_datetime(open_orders['created_at']) < datetime.now() - timedelta(minutes=20)]['id'].tolist()
#        young_ids = open_orders[pd.to_datetime(openOrders['created_at']) >= datetime.now() - timedelta(minutes=20)]['id'].tolist()
#    id_dict = {"young_ids": young_ids, "old_ids": old_ids}
    return(id_dict)



#Main Buy Logic, when the mean of last 5 minutes of 3m SMA is > .3% different from 30m SMA (rolling mean not centered)
#Output can be 'p' for calculation or 'df' for full dataframe.tail
def check_indicator_movement(bw, sw, output):
    
    window_max = max(lower_window, upper_window)


    start_time = datetime.now().replace(microsecond = 0) - timedelta(hours = 1)
    df = ms.get_btc_prices_usd_minute(start_time)

    df['bw'] = df.rolling(window = bw, min_periods = bw)['mean'].mean()
    df['sw'] = df.rolling(window = sw, min_periods = sw)['mean'].mean()
    #Note that this df.tail() returns most recent 5 minutes of data only
    df = df.tail()
    df['p'] = df['sw'] / df['bw']
    p = df['p'].tail(2).mean()
    #logger.info(df)
    if output == 'check':
        p -= 1
        return p
    if output == 'df':
        return df



#Holding pattern to check for changes in p
def indicator_loop(bw, sw, pb, ps, x, p, cur_tick):
    while p > -(x / mult):
        p = indicator_movement(bw, sw, 'check')
        #Magic number, potential profit, should be larger than fees?
        if abs(p) >= (x / mult):
            update_account_info()
            if p < 0 and usdBal > usdMin:
                logger.info("Starting new order, p: " + str(p))
                new_order = bid_buy_btc(cur_tick, pb, ps)
                if len(new_order) > 0:
                    return new_order
                else:
                    logger.warning("USD Bal less than min in dipcheckloop")
                    sleep(60)
                    p = 0
        sleep(1)



# TODO deal with rounding issues!
def strf_float(var, digits):
    var = f'%.{digits}f' % var
    var = str(var)
    return(var)


# Buy order, this currently also gets desired sell price if order is succssful
def bid_buy_btc(cur_tick, pb , ps):
    cur_combo = '{}-USD'.format(cur_tick)
    lb = pc.get_product_ticker(product_id = cur_combo)['price']
    lb = float(lb)
    if lb > my_max:
        logger.info("Current price too high: " + str(lb))
        return []
    # Margins
    buy_limit = lb - (lb * pb * mult)
    btc_amt = (usdBal - keep_usd_in_account) / buy_limit
    btc_amt = strf_btc(btc_amt)
    sell_bid  = lb + (lb * ps * mult)
    buy_limit = strf_bid(buy_limit)
    sell_bid = strf_bid(sell_bid)
    result = None
    while type(result) is not dict:
        result = ac.buy(price = buy_limit, #USD
            size = btc_amt, #BTC
            product_id = cur_combo,
            time_in_force = 'GTT',
            cancel_after = 'hour')
        if result is not dict:
            sleep(1)
    logger.info("BUY: " + btc_amt + "{} @ ".format(cur_tick) + buy_limit + "USD" )
    result['sell_bid'] = sell_bid
    result['sell_size'] = btc_amt
    #logger.info(result)
    return(result)


def bid_sell_btc(bid_size, my_bid, cur_tick):
    cur_combo = '{}-USD'.format(cur_tick)
    result = None
    while type(result) is not dict:
        result = ac.sell(price = my_bid, #USD
            size = bid_size, #BTC
            product_id = cur_combo)
        if result is not dict:
            sleep(1)
    logger.info("Posting Sell Bid: " + my_bid)
    return(result)


def check_fills():
    my_fills = None
    while type(my_fills) is not list:
        try:
            my_fills = ac.get_fills()[0]
        except:
            sleep(1)
    my_fills = pd.DataFrame(my_fills)
    #replace_fills(my_fills)
    return(my_fills)


#Check to see when buy order is successful, timelimit based on GTT
def fill_loop(order, cur_tick):
    exp = datetime.strptime(order['expire_time'], "%Y-%m-%dT%H:%M:%S.%f")
    buyid = order['id']
    logger.info("Entering fill loop, expires: " + str(exp))
    while exp > datetime.now():
        fills = check_fills()
        if buyid in fills.order_id.tolist():
            #Post sell
            bid_sell_btc(order['sell_size'], order['sell_bid'], cur_tick)
            logger.info("SELL: " + str(order['sell_size']) + "BTC @ " + str(order['sell_bid']) + "USD")
            #filled_order = my_find(fills, "order_id", buyid)
            #update_order_db(order, filled_order)
            break
        else:
            sleep(2)


ac = initialize_gdax(False)

signal_id = 'manual'
bot_id = 'manual'




#order_aff = pd.read_sql(sql = f'SELECT * from gdax_order_bot_aff where order_id in ({stale_str})', con = engine)
fills_df = get_gdax_fills(ac)
append_if_new('trade_id', fills_df, 'gdax_fill_hist')
prep_gdax_order_hist(stale_hist)
append_if_new('order_id', stale_hist, 'gdax_order_hist')
append_if_new('order_id', orders_df, 'gdax_orders_cur')

