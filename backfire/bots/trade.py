import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import mysql_prices as ms
from time import sleep
from backfire.bots.gdax_func import get_gdax_stale_new
from backfire.bots.bot_db import update_sql_orders
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

def check_signal_loop(ac, bot_vars, base, quote):
    symbol_pair = f'{base}-{quote}'
    while True:
        # Loops until signal is hit ?
        buy_signal, sell_signal = check_signals(bot_vars, 'signals')
        if (buy_signal + sell_signal) > 0:
            logger.info(f'{bot_vars.bot_id}: Signals: buy_signal: {buy_signal}, sell_signal:{sell_signal}')
            update_db(ac)
            avail_base, avail_quote = avail_balances(bot_vars.bot_id, base_symbol, quote_symbol)
            # TODO: Min xxx should be based on the base, Lookup?
            if buy_signal > 0 and (avail_quote * bot_vars.buy_pct_usd) > bot_vars.min_usd:
                new_order = place_order(ac, symbol_pair, 'buy', bot_vars, avail_base, avail_quote)
                sleep(120)
                update_db(ac)
                #return new_order
            # TODO: Min xxx should be based on the base, Lookup?
            if sell_signal > 0 and (avail_base * bot_vars.sell_pct_usd) > bot_vars.min_btc:
                new_order = place_order(ac, symbol_pair, 'sell', bot_vars, avail_base, avail_quote)
                sleep(120)
                update_db(ac)
                #return new_order
            else:
                logger.warning(f'{bot_vars.bot_id}: Insufficient funds: {base_symbol}TC: {base_bal}, {quote_symbol}: {quote_bal}')
                sleep(120)
        sleep(15)


# Will round down indescriminantly 
def strf_float(my_float, digits):
    var = f'%.{digits}f' % my_float
    my_str = str(var)
    return(my_str)

def place_order(ac, symbol_pair, side, bot_vars, avail_base, avail_quote):
    gdax_public = gdax.PublicClient()
    last_price = gdax_public.get_product_ticker(product_id = symbol_pair)['price']
    last_price = float(last_price)
    # Maagggic numbbbberrrr, avoid market orders but get still filled
    limit_fee = .001 * last_price
    if side == 'sell':
        limit_price = last_price + limit_fee
        total_quote_amt = (avail_quote * bot_vars.pct_buy_usd)
    if side == 'buy':
        limit_price = last_price - limit_fee
        total_quote_amt = (avail_base * bot_vars.pct_buy_btc)
    final_quote_amt = total_quote_amt - (total_quote_amt * bot_vars.gdax_fee_pct)
    base_amt = final_quote_amt / limit_price
    base_amt_str = strf_btc(base_amt, 8)
    limit_price_str = strf_bid(limit_price, 2)
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
            result = ac.sell(price = limit_price, #USD
                size = base_amt, #BTC
                product_id = symbol_pair,
                time_in_force = 'GTT',
                cancel_after = 'hour')
            if result is not dict:
                sleep(1)
    logger.info(f'{symbol_pair}: {side}, base_amt: {base_amt_str}, quote_price: {limit_price_str}')
    return(result)

def update_db(ac):
    new_order_ids, stale_order_ids = get_gdax_stale_new(ac)
    update_sql_orders(new_order_ids, stale_order_ids)


def avail_balances(bot_id, base_symbol, quote_symbol):
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



