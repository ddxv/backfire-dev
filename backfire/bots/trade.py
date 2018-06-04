import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import mysql_prices as ms
from time import sleep
from backfire.bots.gdax_func import get_gdax_stale_new, get_price
from backfire.bots.bot_db import update_sql_orders, avail_balances
import ema_logic
import logging
logger = logging.getLogger(__name__)


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

def check_signal_loop(ac, bot_vars, base_symbol, quote_symbol):
    symbol_pair = f'{base_symbol}-{quote_symbol}'
    while True:
        # Loops until signal is hit ?
        buy_signal, sell_signal = check_signals(bot_vars, 'signals')
        if (buy_signal + sell_signal) > 0:
            logger.info(f'{bot_vars.bot_id}: Signals: buy_signal: {buy_signal}, sell_signal:{sell_signal}')
            update_db(ac)
            avail_base, avail_quote = avail_balances(bot_vars.bot_id, base_symbol, quote_symbol)
            # TODO: Min xxx should be based on the base, Lookup?
            if buy_signal > 0:
                if (avail_quote * bot_vars.buy_pct_usd) > bot_vars.min_usd:
                    new_order = place_order(ac, symbol_pair, 'buy', bot_vars, avail_base, avail_quote)
                    sleep(120)
                    update_db(ac)
                    #return new_order
                else:
                    logger.warning(f'{bot_vars.bot_id}: Insufficient funds: {base_symbol}: {avail_base}, {quote_symbol}: {avail_quote}')
            # TODO: Min xxx should be based on the base, Lookup?
            if sell_signal > 0:
                if (avail_base * bot_vars.sell_pct_btc) > bot_vars.min_btc:
                    new_order = place_order(ac, symbol_pair, 'sell', bot_vars, avail_base, avail_quote)
                    sleep(120)
                    update_db(ac)
                    #return new_order
                else:
                    logger.warning(f'{bot_vars.bot_id}: Insufficient funds: {base_symbol}: {avail_base}, {quote_symbol}: {avail_quote}')
                sleep(120)
        sleep(15)


# Will round down indescriminantly 
def strf_float(my_float, digits):
    var = f'%.{digits}f' % my_float
    my_str = str(var)
    return(my_str)

def place_order(ac, symbol_pair, side, bot_vars, avail_base, avail_quote):
    print(f"availbase,{avail_base}, availquote,{avail_quote}")
    last_price = get_price(symbol_pair)
    # Maagggic numbbbberrrr, avoid market orders but get still filled
    limit_size = -.01 * last_price
    if side == 'sell':
        limit_price = last_price + limit_size
        base_amt = (avail_base * bot_vars.sell_pct_btc)
    if side == 'buy':
        limit_price = last_price - limit_size
        base_amt = (avail_quote * bot_vars.buy_pct_usd) / limit_price
    base_amt_str = strf_float(base_amt, 8)
    limit_price_str = strf_float(limit_price, 2)
    result = None
    if side == 'buy':
        while type(result) is not dict:
            result = ac.buy(price = limit_price_str, #USD
                size = base_amt_str, #BTC
                product_id = symbol_pair,
                time_in_force = 'GTT',
                cancel_after = 'hour')
            if result is not dict:
                sleep(1)
    if side == 'sell':
        while type(result) is not dict:
            result = ac.sell(price = limit_price_str, #USD
                size = base_amt_str, #BTC
                product_id = symbol_pair,
                time_in_force = 'GTT',
                cancel_after = 'hour')
            if result is not dict:
                sleep(1)
    logger.info(f'{symbol_pair}: {side}, base_amt: {base_amt_str}, quote_price: {limit_price_str}')
    print(result)
    return(result)

def update_db(ac):
    new_order_ids, stale_order_ids = get_gdax_stale_new(ac)
    update_sql_orders(new_order_ids, stale_order_ids)


