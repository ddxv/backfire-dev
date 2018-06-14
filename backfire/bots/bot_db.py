import logging
import pandas as pd
import json
from os.path import expanduser
from sqlalchemy import create_engine
logger = logging.getLogger(__name__)

def get_cur_orders():
    cur_orders = pd.read_sql(sql = f'SELECT order_id from gdax_order_cur', con = engine)
    return(cur_orders)


class ConnectDB():
    """docstring for mysql"""
    my_db = None
    engine = None
    def set_engine(self):
        home = expanduser("~")
        secret = open(f'{home}/auth/mysql.auth','r').read()
        my_user = json.loads(secret)['user']
        my_pass = json.loads(secret)['pass']
        try:
            logger.info("Connecting to MySQL database: {self.my_db}")
            self.engine = create_engine(f'mysql://{my_user}:{my_pass}@127.0.0.1:3306/{self.my_db}', echo=False)
        except Exception as error:
            logger.error("Error in class ConnectDB(): {error}")
            self.my_db = None
    # Engine has no close, only engine.connect()
    #def __del__(self):
    #    self.engine.close()


def connect_mysql(my_db):
    home = expanduser("~")
    # MySQL user and pass stored as text json
    secret = open(f'{home}/auth/mysql.auth','r').read()
    my_user = json.loads(secret)['user']
    my_pass = json.loads(secret)['pass']
    # MySQL sqlalchemy engine, connecting to my_db
    engine = create_engine(f'mysql://{my_user}:{my_pass}@127.0.0.1:3306/{my_db}', echo=False)
    return(engine)


def append_if_new(my_id, df, table_name):
    print('db4',db.my_db)
    #engine = connect_mysql()
    my_ids = pd.read_sql(sql = f'SELECT {my_id} from {table_name}', con = db.engine)
    my_ids = my_ids[my_id].tolist()
    new_rows = df[~df[my_id].isin(my_ids)]
    if len(new_rows) > 0:
        new_rows.to_sql(name = table_name,
                con = db.engine,
                if_exists = 'append', index = False)

def get_gdax_fills(ac):
    gdax_fills = ac.get_fills()
    flat_list = [item for sublist in gdax_fills for item in sublist]
    fills_df = pd.DataFrame(flat_list)
    fills_df = fills_df.rename(columns = {'product_id': 'symbol_pair', 'size': 'base_amt', 'usd_volume': 'amt_usd'})
    fills_df['base_symbol'] = fills_df.symbol_pair.str.split('-').str[0]
    fills_df['quote_symbol'] = fills_df.symbol_pair.str.split('-').str[1]
    fills_df['quote_amt'] = pd.to_numeric(fills_df['base_amt']) * pd.to_numeric(fills_df['price'])
    fills_df['quote_amt_inc_fee'] = np.where((fills_df.quote_symbol == 'USD') & (fills_df.side == 'sell'), fills_df['quote_amt'] - pd.to_numeric(fills_df['fee']), fills_df['quote_amt'])
    fills_df['quote_amt_inc_fee'] = np.where((fills_df.quote_symbol == 'USD') & (fills_df.side == 'buy'), fills_df['quote_amt_inc_fee'] + pd.to_numeric(fills_df['fee']), fills_df['quote_amt_inc_fee'])
    fills_df['created_at'] = pd.to_datetime(fills_df['created_at'])
    return(fills_df)

def prep_gdax_order_hist(order_hist):
    order_hist = order_hist.rename(columns = {'id': 'order_id', 'product_id': 'symbol_pair', 'size': 'base_amt', 'type': 'order_type', 'filled_size': 'filled_amt'})
    order_hist['created_at'] = pd.to_datetime(order_hist['created_at'])
    order_cols = ['order_id', 'price', 'base_amt', 'symbol_pair', 'side', 'order_type', 'time_in_force', 'post_only', 'created_at', 'fill_fees', 'filled_amt', 'executed_value', 'status', 'settled']
    order_hist = order_hist[order_cols]
    return(order_hist)


def reset_gdax_orders_manual(uniq_orders):
    bot_id = 'manual'
    signal_id = 'manual'
    order_list = gdax_get_orders(uniq_orders)
    order_hist = pd.DataFrame(order_list)
    order_hist = prep_gdax_order_hist(order_hist)
    order_hist['bot_id'] = bot_id
    order_hist['signal_id'] = signal_id
    append_if_new('order_id', order_hist, 'gdax_order_cur')
    return(order_hist)

def prep_gdax_order_df(orders_list):
    orders_df = pd.DataFrame(orders_list)
    orders_df = orders_df.rename(columns = {'id': 'order_id', 'product_id': 'symbol_pair', 'size': 'base_amt', 'type': 'order_type', 'filled_size': 'filled_amt', 'expire_time': 'expire_at'})
    orders_df['created_at'] = pd.to_datetime(orders_df['created_at'])
    if 'expire_at' in orders_df.columns:
        orders_df['expire_at'] = pd.to_datetime(orders_df['expire_at'])
    return(orders_df)

def gdax_delete_open_orders(order_ids, stale_hist):
    #engine = connect_mysql()
    order_ids_str = ', '.join("'{0}'".format(w) for w in order_ids)
    engine.execute(f"DELETE FROM gdax_order_cur WHERE order_id in ({order_ids_str})")
    append_if_new('order_id', stale_hist, 'gdax_order_hist')


def reset_db(ac):
    # ONLY MANUAL BOT ID
    #### RESET DB
    my_db = 'test'
    update_gdax_transfers(ac)
    fills_df = get_gdax_fills(ac)
    # Should be done programatticallyin bot
    fills_df['bot_id'] = 'manual'
    uniq_orders = fills_df.order_id.unique().tolist()
    order_hist = reset_gdax_orders_manual(uniq_orders)
    update_orders(ac)
    append_if_new('trade_id', fills_df, 'gdax_fill_hist')
    order_hist = prep_gdax_order_hist(order_hist)
    gdax_delete_open_orders(stale_order_ids, order_hist)


def avail_balances(bot_id, base_symbol, quote_symbol):
    #engine = connect_mysql()
    bal_df = pd.read_sql(sql = f"SELECT * FROM gdax_bot_bal WHERE bot_id = '{bot_id}'", con = engine)
    sell_hold = pd.read_sql(sql = f"""SELECT (sum(base_amt) - sum(filled_amt)) as base_hold
            FROM gdax_order_cur
            WHERE bot_id = '{bot_id}' and side = 'sell'
            GROUP BY bot_id
            """, con = engine)
    buy_hold = pd.read_sql(sql = f"""SELECT (base_amt*price) as quote_amt, filled_amt
            FROM gdax_order_cur
            WHERE bot_id = '{bot_id}' and side = 'sell'
            """, con = engine)
    if len(sell_hold) > 0:
        base_hold = sell_hold['base_hold'].sum()
    else:
        base_hold = 0
    if len(buy_hold) > 0:
        #TODO: How is filled_amt handled? is that in base or quote?
        quote_hold = buy_hold['quote_amt'].sum()
    else:
        quote_hold = 0
    avail_base = bal_df[base_symbol.lower()][0] - base_hold
    avail_quote = bal_df[quote_symbol.lower()][0] - quote_hold
    return(avail_base, avail_quote)


db = ConnectDB()

