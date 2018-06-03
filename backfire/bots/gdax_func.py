import gdax
import json
import logging
import mysql_prices as ms
import pandas as pd
logger = logging.getLogger(__name__)

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


def gdax_get_orders(uniq_orders):
    order_list = []
    for o in uniq_orders:
        order = ac.get_order(o)
        order_list.append(order)
    return(order_list)

def get_gdax_stale_new(ac):
    gdax_orders = ac.get_orders()
    gdax_orders = [item for sublist in gdax_orders for item in sublist]
    if len(gdax_orders) > 0:
        orders_df = prep_gdax_order_df(gdax_orders)
        gdax_order_ids = orders_df['order_id'].tolist()
    else:
        gdax_order_ids = gdax_orders
    engine = ms.connect_mysql()
    sql_order_ids = pd.read_sql(sql = f'SELECT order_id from gdax_order_cur', con = engine)
    new_order_ids = set(gdax_order_ids) - set(sql_order_ids['order_id'])
    stale_order_ids = set(sql_order_ids['order_id']) - set(gdax_order_ids)
    return(new_order_ids, stale_order_ids)

def update_gdax_transfers_manual(ac):
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
    append_if_new('transfer_id', transfer_df, 'gdax_transfer_hist')


