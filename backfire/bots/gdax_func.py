import gdax
import json
import logging
from os.path import expanduser
import pandas as pd
from backfire.bots import bot_db
logger = logging.getLogger(__name__)


def load_gdax_auth(test_bool):
    home = expanduser("~")
    if test_bool == True:
        gdax_auth = json.load(open(f'{home}/auth/gdax_sb'))
    if test_bool == False:
        gdax_auth = json.load(open(f'{home}/auth/gdax'))
    key = gdax_auth['key']
    secret = gdax_auth['secret']
    passphrase = gdax_auth['passphrase']
    return(key, secret, passphrase)

def initialize_gdax(test_bool):
    key, secret, passphrase = load_gdax_auth(test_bool)
    if test_bool == True:
        logger.info("Initialize GDAX Sandbox API")
        print('db1',bot_db.db.my_db)
        bot_db.db.my_db = 'gdax_test'
        bot_db.db.set_engine()
        print('db2',bot_db.db.my_db)
        ac = gdax.AuthenticatedClient(key, secret, passphrase,
                api_url="https://api-public.sandbox.gdax.com")
    if test_bool == False:
        logger.info("Initialize live GDAX API")
        bot_db.db.my_db = 'gdax_test'
        bot_db.db.set_engine()
        ac = gdax.AuthenticatedClient(key, secret, passphrase)
    return(ac)


def gdax_get_orders(uniq_orders):
    order_list = []
    for o in uniq_orders:
        order = ac.get_order(o)
        order_list.append(order)
    return(order_list)

def update_orders(ac):
    gdax_orders = ac.get_orders()
    gdax_orders = [item for sublist in gdax_orders for item in sublist]
    if len(gdax_orders) > 0:
        orders_df = bot_db.prep_gdax_order_df(gdax_orders)
        gdax_order_ids = orders_df['order_id'].tolist()
    else:
        gdax_order_ids = gdax_orders
    #engine = db.connect_mysql()
    sql_order_ids = bot_db.get_cur_orders()
    new_order_ids = set(gdax_order_ids) - set(sql_order_ids['order_id'])
    stale_order_ids = set(sql_order_ids['order_id']) - set(gdax_order_ids)
    # Add new
    if len(new_order_ids) > 0:
        new_orders_df = orders_df[orders_df['order_id'].isin(new_order_ids)]
        bot_db.append_if_new('order_id', new_orders_df, 'gdax_order_cur')
    # Remove old
    if len(stale_order_ids) > 0:
        stale_hist = gdax_get_orders(stale_order_ids)
        stale_hist = pd.DataFrame(stale_hist)
        stale_hist = prep_gdax_order_hist(stale_hist)
        fills_df = get_gdax_fills(ac)
        bot_db.append_if_new('trade_id', fills_df, table_name)
        gdax_delete_open_orders(stale_order_ids, stale_hist)


def update_gdax_transfers_manual(ac):
    print('db3',bot_db.db.my_db)
    bot_id = 'manual'
    signal_id = 'manual'
    my_accounts = ac.get_accounts()
    transfer_list = []
    for i in my_accounts:
        my_id = i['id']
        my_cur = i['currency']
        gdax_acc_hist = ac.get_account_history(my_id)
        gdax_acc_hist = [item for sublist in gdax_acc_hist for item in sublist]
        for d in gdax_acc_hist:
            if d['type'] == 'transfer':
                d['cur'] = my_cur
                d = {**d, **d.pop('details', None)}
                transfer_list.append(d)
    transfer_df = pd.DataFrame(transfer_list)
    transfer_df['signal_id'] = signal_id
    transfer_df['bot_id'] = bot_id
    transfer_df = transfer_df.rename(columns = {'amount': 'transfer_amt', 'id': 'trade_id'})
    transfer_df = transfer_df[['transfer_amt', 'created_at', 'cur', 'trade_id', 'transfer_id', 'transfer_type', 'bot_id']]
    transfer_df['created_at'] = pd.to_datetime(transfer_df['created_at'])
    bot_db.append_if_new('transfer_id', transfer_df, 'gdax_transfer_hist')

def get_price(symbol_pair):
    gdax_public = gdax.PublicClient()
    last_price = gdax_public.get_product_ticker(product_id = symbol_pair)['price']
    last_price = float(last_price)
    return(last_price)

