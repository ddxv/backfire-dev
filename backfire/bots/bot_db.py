import logging
logger = logging.getLogger(__name__)

def append_if_new(my_id, df, table_name):
    engine = ms.connect_mysql()
    my_ids = pd.read_sql(sql = f'SELECT {my_id} from {table_name}', con = engine)
    my_ids = my_ids[my_id].tolist()
    new_rows = df[~df[my_id].isin(my_ids)]
    if len(new_rows) > 0:
        new_rows.to_sql(name = table_name,
                con = engine,
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
    orders_df = orders_df.rename(columns = {'id': 'order_id', 'product_id': 'symbol_pair', 'size': 'base_amt', 'type': 'order_type', 'filled_size': 'filled_amt'})
    orders_df['created_at'] = pd.to_datetime(orders_df['created_at'])
    return(orders_df)

def gdax_delete_open_orders(order_ids, stale_hist):
    engine = ms.connect_mysql()
    order_ids_str = ', '.join("'{0}'".format(w) for w in order_ids)
    engine.execute(f"DELETE FROM gdax_order_cur WHERE order_id in ({order_ids_str})")
    append_if_new('order_id', stale_hist, 'gdax_order_hist')


def reset_db():
    # ONLY MANUAL BOT ID
    #### RESET DB
    update_gdax_transfers(ac)
    fills_df = get_gdax_fills(ac)
    # Should be done programatticallyin bot
    fills_df['bot_id'] = 'manual'
    uniq_orders = fills_df.order_id.unique().tolist()
    order_hist = reset_gdax_orders_manual(uniq_orders)
    new_order_ids, stale_order_ids = get_gdax_stale_new(ac)
    append_if_new('trade_id', fills_df, 'gdax_fill_hist')
    order_hist = prep_gdax_order_hist(order_hist)
    gdax_delete_open_orders(stale_order_ids, order_hist)


def update_sql_orders(new_order_ids, stale_order_ids):
    # Add new
    if len(new_order_ids) > 0:
        #new_orders_df = gdax_orders[gdax_orders['order_id'].isin(new_orders)]
        append_if_new('order_id', gdax_orders, 'gdax_order_cur')
    # Remove old
    if len(stale_order_ids) > 0:
        stale_hist = gdax_get_orders(stale_order_ids)
        stale_hist = pd.DataFrame(stale_hist)
        stale_hist = prep_gdax_order_hist(stale_hist)
        fills_df = get_gdax_fills(ac)
        append_if_new('trade_id', fills_df, table_name)
        gdax_delete_open_orders(stale_order_ids, stale_hist)

def select_all_from(my_table):
    engine = ms.connect_mysql()
    df = pd.read_sql(sql = f'SELECT * FROM {my_table}', con = engine)
    return(df)
