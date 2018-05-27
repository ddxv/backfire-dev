import gdax
import json
from datetime import datetime, timedelta
import pandas as pd
import mysql_prices as ms
from time import sleep
import logging
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

class AccountBalances:
    usd = 0
    usd_hold = 0
    btc = 0
    btc_hold = 0
    def sub_btc(self, btc_amt):
        self.btc -= btc_amt
    def sub_usd(self, usd_amt):
        self.usd -= usd_amt
    def add_btc(self, btc_amt):
        self.btc += btc_amt
    def add_usd(self, usd_amt):
        self.usd += usd_amt
    def set_usd(self, new_bal):
        self.usd = new_bal
    def set_btc(self, new_bal):
        self.btc = new_bal
    def set_usd_hold(self, new_bal):
        self.usd_hold = new_bal
    def set_btc_hold(self, new_bal):
        self.btc_hold = new_bal

class AlgoSettings:
    upper_window = 0
    def set_upper_window(self, val):
        self.upper_window = val
    lower_window = 0
    def set_lower_window(self, val):
        self.lower_window = val
    factor_high = 0
    def set_factor_high(self, val):
        self.factor_high = val
    factor_low = 0
    def set_factor_low(self, val):
        self.factor_low = val
    buy_pct_usd = 0
    def set_buy_pct_usd(self, val):
        self.buy_pct_usd = val
    sell_pct_btc = 0
    def set_sell_pct_btc(self, val):
        self.sell_pct_btc = val
    principle_btc = 0
    def set_principle_btc(self, val):
        self.principle_btc = val
    principle_usd = 0
    def set_principle_usd(self, val):
        self.principle_usd = val
    min_usd = 0
    def set_min_usd(self, val):
        self.min_usd = val
    min_btc = 0
    def set_min_btc(self, val):
        self.min_btc = val


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

def initialize_account_info(acc):
    btc_acc_id = my_find(my_accounts, 'currency', 'BTC')['profile_id']
    usd_acc_id = my_find(my_accounts, 'currency', 'USD')['profile_id']
    #??

#Global variables for available account balances
def update_account_info(acc):
    my_accounts = gdax_client.get_accounts()
    while type(my_accounts) is not list:
        logger.warning("Retrying get_accounts")
        sleep(1)
        my_accounts = ac.get_accounts()
    acc.set_btc(float(my_find(my_accounts, 'currency', 'BTC')['available']))
    acc.set_usd(float(my_find(my_accounts, 'currency', 'USD')['available']))
    acc.set_btc_hold(float(my_find(my_accounts, 'currency', 'BTC')['hold']))
    acc.set_usd_hold(float(my_find(my_accounts, 'currency', 'USD')['hold']))
    return(acc)

#def replace_fills(fills):
#    if not joker:
#        con = ms.connect_mysql()
#        fills.to_sql('test_fills',con, if_exists="replace", index=False)

#Simply returns open orders
def get_open_orders():
    my_orders = ac.get_orders()
    while type(my_orders) is not list:
        sleep(2)
        my_orders = ac.get_orders()
    if len(my_orders[0]) == 0:
        young_ids = []
        old_ids = []
    else:
        order_df = pd.DataFrame(my_orders[0])
        open_orders = order_df[order_df['status'] == 'open']
        old_ids = open_orders[pd.to_datetime(open_orders['created_at']) < datetime.now() - timedelta(minutes=20)]['id'].tolist()
        young_ids = open_orders[pd.to_datetime(openOrders['created_at']) >= datetime.now() - timedelta(minutes=20)]['id'].tolist()
    id_dict = {"young_ids": young_ids, "old_ids": old_ids}
    return(id_dict)


##List of Order IDs to kill
#def kill_orders(ids):
#   for i in ids:
#       if i not in long_orders:
#           ac.cancel_order(i)

#Main Buy Logic, when the mean of last 5 minutes of 3m SMA is > .3% different from 30m SMA (rolling mean not centered)
#Output can be 'p' for calculation or 'df' for full dataframe.tail
def check_rolling_movement(bw, sw, output):
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
def dip_check_loop(bw, sw, pb, ps, x, p, cur_tick):
    while p > -(x / mult):
        p = check_rolling_movement(bw, sw, 'check')
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



##Bids must be submitted as strings with nomore than two decimal points
#def strf_bid(bid):
#    bid = "%.2f" % bid
#    bid = str(bid)
#    return(bid)

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



def update_order_db(order, fill):
   fill['principal'] = float(fill['fee']) + (float(fill['price']) * float(fill['size']))
   fill['breakeven'] = fill['principal'] / float(fill['size'])
   fill['sell_bid'] = order['sell_bid']
   fill = pd.DataFrame([fill])
   if not joker:
       con = ms.connect_mysql()
       fill.to_sql('buy_btc_orders',con, if_exists="append", index=False)



def update_sell_db(order, fill):
    all=pd.DataFrame(ac.get_orders()[0])
    all.created_at = pd.to_datetime(all.created_at)
    cutoff = datetime.now() - timedelta(hours=1)
    stale_sells = all.loc[(all['side']=='sell') & (all['created_at'] < cutoff)].id
    for id in stale_sells:
        logger.info("stale sell orders: " + str(stale_sells.values.tolist()))





def append_fills_table(fills_df, table_name):
    fills_df['created_at'] = pd.to_datetime(fills_df['created_at'])
    trade_ids = pd.read_sql(sql = f'SELECT trade_id from {table_name}', con = engine)
    trade_ids = trade_ids['trade_id'].tolist()
    new_rows = fills_df[~fills_df['trade_id'].isin(trade_ids)]
    if len(new_rows) > 0:
        new_rows.to_sql(name = table_name,
                con = engine,
                if_exists = 'append', index = False)

def get_gdax_fills(ac):
    gdax_fills = ac.get_fills()
    flat_list = [item for sublist in gdax_fills for item in sublist]
    fills_df = pd.DataFrame(flat_list)
    fills_df = fills_df.rename(columns = {'product_id': 'symbol_pair', 'size': 'amt', 'usd_volume': 'amt_usd'})
    return(fills_df)


engine = ms.connect_mysql()
table_name = 'gdax_fills'

fills_df = get_gdax_fills(ac)

fills_df['signal_id'] = signal_id
fills_df['alg_id'] = alg_id

append_fills_table(fills_df, 'gdax_fills')

engine = ms.connect_mysql()
table_name = 'gdax_orders'



    gdax_fills = ac.get_fills()
    flat_list = [item for sublist in gdax_fills for item in sublist]
    fills_df = pd.DataFrame(flat_list)
    fills_df = fills_df.rename(columns = {'product_id': 'symbol_pair', 'size': 'amt', 'usd_volume': 'amt_usd'})
    return(fills_df)


