import logging
import pandas as pd
import json
from os.path import expanduser
from sqlalchemy import create_engine
logger = logging.getLogger(__name__)

def get_cur_orders():
    cur_orders = pd.read_sql(sql = f'SELECT order_id from gdax_order_cur', con = db.engine)
    return(cur_orders)

def get_order_aff():
    aff = pd.read_sql(sql = f'SELECT * from gdax_order_aff', con = db.engine)
    return(aff)


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
            logger.info(f'Connecting to MySQL database: {self.my_db}')
            self.engine = create_engine(f'mysql://{my_user}:{my_pass}@127.0.0.1:3306/{self.my_db}', echo=False)
        except Exception as error:
            logger.error(f'Error in class ConnectDB(): {error}')
            self.my_db = None
    # Engine has no close, only engine.connect()
    #def __del__(self):
    #    self.engine.close()



def insert_dict(table_name, my_dict):
    placeholder = ", ".join(["%s"] * len(my_dict))
    statement = f"""insert into `{table_name}` ({",".join(my_dict.keys())}) values ({placeholder});"""
    db.engine.execute(statement, list(my_dict.values()))


def append_if_new(my_id, df, table_name):
    my_ids = pd.read_sql(sql = f'SELECT {my_id} from {table_name}', con = db.engine)
    my_ids = my_ids[my_id].tolist()
    new_rows = df[~df[my_id].isin(my_ids)]
    if len(new_rows) > 0:
        new_rows.to_sql(name = table_name,
                con = db.engine,
                if_exists = 'append', index = False)

def prep_gdax_order_hist(order_hist):
    order_hist = order_hist.rename(columns = {'id': 'order_id', 'product_id': 'symbol_pair', 'size': 'base_amt', 'type': 'order_type', 'filled_size': 'filled_amt'})
    order_hist['created_at'] = pd.to_datetime(order_hist['created_at'])
    order_cols = ['order_id', 'price', 'base_amt', 'symbol_pair', 'side', 'order_type', 'time_in_force', 'post_only', 'created_at', 'fill_fees', 'filled_amt', 'executed_value', 'status', 'settled']
    order_hist = order_hist[order_cols]
    return(order_hist)

def prep_gdax_order_df(orders_list):
    orders_df = pd.DataFrame(orders_list)
    orders_df = orders_df.rename(columns = {'id': 'order_id', 'product_id': 'symbol_pair', 'size': 'base_amt', 'type': 'order_type', 'filled_size': 'filled_amt', 'expire_time': 'expire_at'})
    orders_df['created_at'] = pd.to_datetime(orders_df['created_at'])
    orders_df = orders_df.drop('funds',axis=1, errors='ignore')
    orders_df = orders_df.drop('specified_funds',axis=1, errors='ignore')
    if 'expire_at' in orders_df.columns:
        orders_df['expire_at'] = pd.to_datetime(orders_df['expire_at'])
        orders_df['done_at'] = pd.to_datetime(orders_df['done_at'])
    return(orders_df)

def gdax_delete_open_orders(order_ids, stale_hist):
    order_ids_str = ', '.join("'{0}'".format(w) for w in order_ids)
    db.engine.execute(f"DELETE FROM gdax_order_cur WHERE order_id in ({order_ids_str})")
    append_if_new('order_id', stale_hist, 'gdax_order_hist')



def avail_balances(bot_id, base_symbol, quote_symbol):
    bal_df = pd.read_sql(sql = f"SELECT * FROM gdax_bot_bal WHERE bot_id = '{bot_id}'", con = db.engine)
    sell_hold = pd.read_sql(sql = f"""SELECT (sum(base_amt) - sum(filled_amt)) as base_hold
            FROM gdax_order_cur
            WHERE bot_id = '{bot_id}' and side = 'sell'
            GROUP BY bot_id
            """, con = db.engine)
    buy_hold = pd.read_sql(sql = f"""SELECT (base_amt*price) as quote_amt, filled_amt
            FROM gdax_order_cur
            WHERE bot_id = '{bot_id}' and side = 'sell'
            """, con = db.engine)
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

