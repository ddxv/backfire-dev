import uuid
from datetime import datetime, timedelta
import pandas as pd
import mysql_prices as ms
from time import sleep
from backfire.bots.gdax_func import get_price, update_orders
from backfire.bots import bot_db
import ema_logic
import logging
logger = logging.getLogger(__name__)

class NewSignal():
    signal_id = None
    side = None
    symbol_pair = None
    avail_quote = None
    avail_base = None
    bot_id = None
    created_at = None


def check_signals(bot_vars, output):
    window_max = max(bot_vars.lower_window, bot_vars.upper_window)
    start_time = datetime.now().replace(microsecond = 0) - timedelta(minutes = (window_max+20000))
    df = ms.get_gdax_prices(start_time)
    df = ema_logic.set_signals(df, bot_vars).tail(25)
    res_df = df.tail(2)[['buy_signal', 'sell_signal']].sum()
    buy_signal = res_df['buy_signal']
    sell_signal = res_df['sell_signal']
    if output == 'signals':
        return(buy_signal, sell_signal)
    if output == 'df':
        return(df)


def create_signal_dict(signal):
    signal_dict = {
            'signal_id': signal.signal_id, 
            'bot_id': signal.bot_id,
            'side': signal.side,
            'created_at': signal.created_at,
            }
    return(signal_dict)

def check_signal_loop(ac, bot_vars, base_symbol, quote_symbol):
    symbol_pair = f'{base_symbol}-{quote_symbol}'
    while True:
        buy_signal, sell_signal = check_signals(bot_vars, 'signals')
        if (buy_signal + sell_signal) > 0:
            logger.info(f"""{bot_vars.bot_id}: Signals: buy_signal: {buy_signal}, sell_signal:{sell_signal}""")
            update_db(ac)
            signal = NewSignal()
            signal.created_at = datetime.now()
            signal.avail_base, signal.avail_quote = bot_db.avail_balances(bot_vars.bot_id, base_symbol, quote_symbol)
            signal.bot_id = bot_vars.bot_id
            signal.symbol_pair = symbol_pair
            # TODO: Min xxx should be based on the base, Lookup?
            if buy_signal > 0:
                signal.signal_id = uuid.uuid4()
                signal.side = 'buy'
                signal_dict = create_signal_dict(signal)
                bot_db.insert_dict('gdax_signal_hist', signal_dict)
                if (signal.avail_quote * bot_vars.buy_pct_usd) > bot_vars.min_usd:
                    new_order = place_order(ac, signal, bot_vars)
                    bot_db.insert_dict('gdax_order_cur', new_order)
                    sleep(120)
                    update_db(ac)
                else:
                    logger.warning(f"""{bot_vars.bot_id}: Insufficient funds: {base_symbol}: {signal.avail_base}, {quote_symbol}: {signal.avail_quote}""")
            # TODO: Min xxx should be based on the base, Lookup?
            if sell_signal > 0:
                signal.signal_id = uuid.uuid4()
                signal.side = 'sell'
                signal_dict = create_signal_dict(signal)
                bot_db.insert_dict('gdax_signal_hist', signal_dict)
                if (signal.avail_base * bot_vars.sell_pct_btc) > bot_vars.min_btc:
                    new_order = place_order(ac, signal, bot_vars)
                    bot_db.insert_dict('gdax_order_cur', new_order)
                    sleep(120)
                    update_db(ac)
                else:
                    logger.warning(f"""{bot_vars.bot_id}: Insufficient funds: {base_symbol}: {signal.avail_base}, {quote_symbol}: {signal.avail_quote}""")
                sleep(120)
        sleep(15)


def prep_order_dict(my_dict, signal):
    print(my_dict)
    my_dict['created_at'] = pd.to_datetime(my_dict['created_at'])
    my_dict['signal_id'] = signal.signal_id
    my_dict['bot_id'] = signal.bot_id
    my_dict['order_id'] = my_dict.pop('id')
    my_dict['base_amt'] = my_dict.pop('size')
    my_dict['symbol_pair'] = my_dict.pop('product_id')
    my_dict['order_type'] = my_dict.pop('type')
    my_dict['expire_at'] = my_dict.pop('expire_time')
    my_dict['filled_amt'] = my_dict.pop('filled_size')
    del my_dict['stp']
    return(my_dict)

# Will round down indescriminantly 
def strf_float(my_float, digits):
    var = f'%.{digits}f' % my_float
    my_str = str(var)
    return(my_str)

def place_order(ac, signal, bot_vars):
    last_price = get_price(signal.symbol_pair)
    # Maagggic numbbbberrrr, avoid market orders but get still filled
    limit_size = -.01 * last_price
    if signal.side == 'sell':
        limit_price = last_price + limit_size
        base_amt = (signal.avail_base * bot_vars.sell_pct_btc)
    if signal.side == 'buy':
        limit_price = last_price - limit_size
        base_amt = (signal.avail_quote * bot_vars.buy_pct_usd) / limit_price
    base_amt_str = strf_float(base_amt, 8)
    limit_price_str = strf_float(limit_price, 2)
    result = None
    if signal.side == 'buy':
        while type(result) is not dict:
            result = ac.buy(price = limit_price_str, #USD
                size = base_amt_str, #BTC
                product_id = signal.symbol_pair,
                time_in_force = 'GTT',
                cancel_after = 'hour')
            if result is not dict:
                logger.warning(f""" GDAX API returned None, will try again for {signal.symbol_pair}: {signal.side}, base_amt: {base_amt_str}, quote_price: {limit_price_str}""")
                sleep(1)
    if signal.side == 'sell':
        while type(result) is not dict:
            result = ac.sell(price = limit_price_str, #USD
                size = base_amt_str, #BTC
                product_id = signal.symbol_pair,
                time_in_force = 'GTT',
                cancel_after = 'hour')
            if result is not dict:
                logger.warning(f"""GDAX API returned None, will try again for {signal.symbol_pair}: {signal.side}, base_amt: {base_amt_str}, quote_price: {limit_price_str}""")
                sleep(1)
    logger.info(f"""{signal.symbol_pair}: {signal.side}, base_amt: {base_amt_str}, quote_price: {limit_price_str}""")
    order = prep_order_dict(result, signal)
    return(order)

def update_db(ac):
    update_orders(ac)
    #new_order_ids, stale_order_ids = get_gdax_stale_new(ac)


