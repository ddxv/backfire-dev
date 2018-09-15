from multiprocessing import Pool
from functools import partial
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
import logging
import numpy as np
import time
import sys
import itertools
logger = logging.getLogger(__name__)

class AccountBalances:
    """ AccountBalances manages btc/usd balances used in run_backest
    contains functions for subtract / add / set for each currency
Functions
    ---------
    WARNING: THESE FUNCTIONS WILL BE DEPRECIATED:
        BETTER WAY TO SET VARIABLES:
        bal = AccountBalances()
        bal.usd = 55.34

    def sub_cur(self, val)
        subtracts the val from the cur

    def add_cur(self, val)
        adds the val from the cur

    def set_cur(self, val)
        sets the val from the cur

    Parameters
    ----------
    btc : numerical, default 0
                    Balance of BTC
    usd : numerical, default 0
                    Balance of USD
    """
    # __init__ will initiate new class each time
    def __init__(self, p_btc, p_usd):
        self.usd = p_usd
        self.btc = p_btc


class BacktestSettings:
    def __init__(self):
        self.principle_btc = 0
        self.principle_usd = 0
        upper_window = 0
        lower_window = 0
        factor_high = 0
        factor_low = 0
        buy_pct_usd = 0
        sell_pct_btc = 0
        min_usd = 0
        min_btc = 0
        start_date = 0
        end_date = 0
    def set_principle_btc(self, val):
        self.principle_btc = val
    def set_principle_usd(self, val):
        self.principle_usd = val
    def set_upper_window(self, val):
        self.upper_window = val
    def set_lower_window(self, val):
        self.lower_window = val
    def set_factor_high(self, val):
        self.factor_high = val
    def set_factor_low(self, val):
        self.factor_low = val
    def set_buy_pct_usd(self, val):
        self.buy_pct_usd = val
    def set_sell_pct_btc(self, val):
        self.sell_pct_btc = val
    def set_min_usd(self, val):
        self.min_usd = val
    def set_min_btc(self, val):
        self.min_btc = val
    def set_start_date(self, val):
        self.start_date = val
    def set_end_date(self, val):
        self.end_date = val


def run_backtest(df, desired_outputs, bt):
    """ run_backtest loops over the rows of buys and sells in df. 
    It calculates buys and sells and keeps a running balance of inputs. 
    Outputs a simplified dictionary of the results 
    or a DataFrame of all successfull fills.
    
    ----------
    df : DataFrame, 
                    Only needs to be buy and sells with other data removed
                    to increase speed
    desired_outputs: string, default "both"
                    Toggles simple dictionary of results or 
                    df of fill data
    bt : Class: BacktestSettings(),
                    Contatins all required variables for running backest
    """
    bal = AccountBalances(bt.principle_btc, bt.principle_usd)
    fills = []
    #for row in list(zip(df['timestamp'], df['close'], df['buy_signal'], df['sell_signal'])):
    for row in df.itertuples(index=False):
        price = row[1]
        # BUY SIGNAL
        if row[2] == 1 and ((bal.usd * bt.buy_pct_usd) > bt.min_usd):
            usd_spend_amt = bal.usd * bt.buy_pct_usd
            btc_receive_amt = usd_spend_amt / price
            btc_receive_amt = btc_receive_amt - (btc_receive_amt * .001)
            bal.btc += btc_receive_amt
            bal.usd -= usd_spend_amt
            running_bal = bal.usd + (bal.btc * price)
            fills.append(create_fill(row[0], 'buy', price, btc_receive_amt, usd_spend_amt, running_bal))
        # SELL SIGNAL
        if row[3] == 1 and (bal.btc * bt.sell_pct_btc) > bt.min_btc:
            btc_spend_amt = bal.btc * bt.sell_pct_btc
            usd_receive_amt = price * btc_spend_amt
            usd_receive_amt = usd_receive_amt - (usd_receive_amt * .001)
            bal.usd += usd_receive_amt
            bal.btc -= btc_spend_amt
            running_bal = bal.usd + (bal.btc * price)
            fills.append(create_fill(row[0], 'sell', price, btc_spend_amt, usd_receive_amt, running_bal))
    num_fills = len(fills)

    # draw down calculation:
    if desired_outputs == 'backtest':
        fills = pd.DataFrame(fills)
        # enough fills to calculate it?
        if len(fills) > 0:
            dd_max_usd, dd_duration, dd_percent = create_drawdowns(fills['running_bal'])
        else:
            dd_max_usd = 0
            dd_duration = 0
            dd_percent = 0

    result = {
            "n_fills": num_fills,
            "upper_window": bt.upper_window,
            "lower_window": bt.lower_window,
            "upper_factor": bt.factor_high,
            "lower_factor": bt.factor_low,
            "buy_pct_usd": bt.buy_pct_usd,
            "sell_pct_btc": bt.sell_pct_btc,
            "usd_bal": bal.usd,
            "btc_bal": bal.btc,
            "drawdown": dd_percent,
            "dd_duration": dd_duration
            }
    if desired_outputs == "both":
#        fills = pd.DataFrame(fills)
        return result, fills
    else:
        return result


def run_backtest_reduced(df, bt):
    """
    Same as above but more reduced for faster iteration of millions of tests
    """
    bal = AccountBalances(bt.principle_btc, bt.principle_usd)
    fills = []
    n_buys = 0
    n_sells = 0
    #for row in list(zip(df['timestamp'], df['close'], df['buy_signal'], df['sell_signal'])):
    for row in df.itertuples(index=False):
        price = row[1]
        # BUY SIGNAL
        if row[2] == 1 and ((bal.usd * bt.buy_pct_usd) > bt.min_usd):
            usd_spend_amt = bal.usd * bt.buy_pct_usd
            btc_receive_amt = usd_spend_amt / price
            btc_receive_amt = btc_receive_amt - (btc_receive_amt * .001)
            bal.btc += btc_receive_amt
            bal.usd -= usd_spend_amt
            running_bal = bal.usd + (bal.btc * price)
            n_buys += 1
        # SELL SIGNAL
        if row[3] == 1 and (bal.btc * bt.sell_pct_btc) > bt.min_btc:
            btc_spend_amt = bal.btc * bt.sell_pct_btc
            usd_receive_amt = price * btc_spend_amt
            usd_receive_amt = usd_receive_amt - (usd_receive_amt * .001)
            bal.usd += usd_receive_amt
            bal.btc -= btc_spend_amt
            running_bal = bal.usd + (bal.btc * price)
            n_sells += 1
    num_fills = n_buys + n_sells


    result = {
            "n_fills": num_fills,
            "n_buys": n_buys,
            "n_sells": n_sells,
            "upper_window": bt.upper_window,
            "lower_window": bt.lower_window,
            "upper_factor": bt.factor_high,
            "lower_factor": bt.factor_low,
            "buy_pct_usd": bt.buy_pct_usd,
            "sell_pct_btc": bt.sell_pct_btc,
            "usd_bal": bal.usd,
            "btc_bal": bal.btc
            }
    return result


def create_fill(my_time, my_side, btc_price, btc_val, usd_val, running_bal):
    result = {}
    result['timestamp'] = my_time
    result['side'] = my_side
    result['price'] = btc_price
    result['btc_val'] = btc_val
    result['usd_val'] = usd_val
    result['running_bal'] = running_bal
    return(result)

# returns a time in the past with correct offset so the ema line can calculate correctly before our
# actual backtestin start date.
# the formula is: start_time - (base_length * ema_period_lenght) as minutes
# we should probably do a check here somewhere in the future to prevent times in the past which
# are beyond our available data set.
def get_start_time_for_ema(ema_length, start_time):
        # base multiplier for our ema length
        base_length = 100
        # start date is offset into the past by the ema length * base length
        new_start = datetime.strptime(start_time, "%Y-%m-%d") - pd.DateOffset(minutes=(ema_length * base_length))
        # return the new date as a str
        return str(new_start)

def prep_data(raw_data, start_time, end_time):
    trimmed_df = raw_data[raw_data.timestamp >= start_time]
    trimmed_df = trimmed_df[trimmed_df.timestamp <= end_time]
    trimmed_df = trimmed_df.reset_index(drop = True)
    day_df = trimmed_df.groupby(trimmed_df['timestamp'].dt.date).agg({'open':'first',  'high': max, 'low': min, 'close':'last',}).reset_index()
    return(day_df, trimmed_df)

def prep_data_old(raw_data, start_time, end_time):
    #raw_data = pd.read_csv('~/coinbase_data.csv')
    raw_data['timestamp'] = pd.to_datetime(raw_data.timestamp)
    trimmed_df = raw_data[raw_data.timestamp >= start_time]
    trimmed_df = trimmed_df[trimmed_df.timestamp <= end_time]
    trimmed_df = trimmed_df.reset_index(drop = True)
    day_df = trimmed_df.groupby(trimmed_df['timestamp'].dt.date).agg({'open':'first',  'high': max, 'low': min, 'close':'last',}).reset_index()
    return(day_df, trimmed_df)

def add_vectorized_cols(df, results, bt_vars):
#    results['sd'] = bt_vars.start_date
#    results['ed'] = bt_vars.end_date
#    results['p_usd'] = bt_vars.principle_usd
#    results['p_btc'] = bt_vars.principle_btc
    first_close = df[0:1]['close'].values[0]
    final_close = df.tail(2)[0:1]['close'].values[0]
#    results['open'] = first_close
#    results['close'] = final_close
    hodl_btc_total = (bt_vars.principle_usd / first_close) + bt_vars.principle_btc
    total_usd_principle = hodl_btc_total * first_close
    hodl_usd_final = hodl_btc_total * final_close
    hodl_roi = (hodl_usd_final - total_usd_principle) / total_usd_principle
    results['hodl_roi'] = hodl_roi
    results['final_bal'] = results['usd_bal'] + (final_close * results['btc_bal'])
    results['roi'] = (results['final_bal'] - total_usd_principle) / total_usd_principle
# draw down calculation:
#dd_max_usd, dd_duration, dd_percent = create_drawdowns(my_fills['running_bal'])
#results['drawdown'] = dd_percent
#results['dd_duration'] = dd_duration
    return(results)


def single_backtest(df, bt_vars):
    lw = bt_vars.lower_window
    uw = bt_vars.upper_window
    lf = bt_vars.factor_low
    uf = bt_vars.factor_high
    lower_window = f'lower_{lw}_{lf}'
    upper_window = f'upper_{uw}_{uf}'
    # TODO: Can these trimming be moved to prep data?
    #df = df[df.timestamp >= pd.to_datetime(bt_vars.start_date)]
    #df = df[df.timestamp <= pd.to_datetime(bt_vars.end_date)]
    df[lower_window] = df.close.ewm(span=lw, adjust = False).mean() * (1 - lf)
    df[upper_window] = df.close.ewm(span=uw, adjust = False).mean() * (uf + 1)
    df = ema_crosses(df, lower_window, upper_window)
    results, my_fills = run_backtest(df, 'both', bt_vars)
    my_fills = fills_running_bal(my_fills, bt_vars)
    results = add_vectorized_cols(df, results, bt_vars)
    return(df, results, my_fills)

total_left = 0
time_last_start = 0
next_print = 0

def run_multi(df, result_type, bt, my_data, totalfl):
    """ run_multi runs parallelized backtests on a iterable my_data
    and sets buy & sell signals.
    These signals are sent as ang df to run_backtest
    The output of run_multi is the result from run_backtest

    Parameters
    ----------
    df : pd.DataFrame, must contain 'close' and 'timestamp' columns
        timestamp: pandas datetime object
        close: numerical
    result_type: for multi it musts be 'both'
    bt : BacktestSettings() Class, a base is passed in,
                remaining values are filled in from my_data
    my_data : a product matrix of all possible variables to be run
            order in which values are put into the my_data 
            represents their index
    """

    global total_left    
    global time_last_start
    global next_print
    totalfl = int(totalfl / 4) #divide by 4 because of 4 pool threads
    total_left = totalfl
    next_print = totalfl + 1
    time_last_start = time.time()

    res_list = []
    with Pool(4) as p:
        my_partial_func = partial(parallelized_backtest, df, result_type, bt, totalfl)
        result_list = p.map(my_partial_func, my_data)
        res_list.append(result_list)
    res_df = pd.concat([pd.DataFrame(d) for d in res_list]).reset_index(drop = True)
    res_df = add_vectorized_cols(df, res_df, bt)
    return(res_df)

def run_multi_v2(df, bt, my_data):
    res_list = []
    with Pool(4) as p:
        my_partial_func = partial(parallelized_backtest_v2, df, bt)
        result_list = p.map(my_partial_func, my_data)
        res_list.append(result_list)
    res_df = pd.concat([pd.DataFrame(d) for d in res_list]).reset_index(drop = True)
    res_df = add_vectorized_cols(df, res_df, bt)
    return(res_df)


def ema_crosses(df, lower_window, upper_window):
    df['buy_signal'] = np.where(((df.close < df[lower_window]) & (df.close.shift(1) > df[lower_window].shift(1))), 1, 0)
    df['sell_signal'] = np.where(((df.close > df[upper_window]) & (df.close.shift(1) < df[upper_window].shift(1))), 1, 0)
    return(df)


def parallelized_backtest(df, result_type, bt, totalfl, my_data):
    """
    global total_left
    global time_last_start
    global next_print
    total_left -= 1
    #estimate how much time left
    
    if (next_print >= total_left):
        #seconds_left = ((time.process_time() - time_last_start) / (totalfl - total_left)) * total_left
        #print('             ', total_left, 'sets remain and', round(seconds_left / 60 , 3), 'minutes left...', end='\r')
        time_elapsed = time.time() - time_last_start
        items_calculated = int(totalfl - total_left)
        time_per_set = time_elapsed / items_calculated
        sstr = '['+str(total_left)+'/'+str(totalfl)+'] time_elapsed='+str(round(time_elapsed,3))+' items_calculated='+str(items_calculated)+' seconds per set='+str(round(time_per_set, 4))+' estimate minutes left='+str(round((time_per_set*total_left) / 60 , 3))
        print(sstr, end='\r')
        next_print -= 1
    """ 
    bt.upper_window = my_data[0]
    bt.lower_window = my_data[1]
    bt.factor_high = my_data[2]
    bt.factor_low = my_data[3]
    bt.buy_pct_usd = my_data[4]
    bt.sell_pct_btc = my_data[5]
    # trim results and ignore fills below on our start date:
    upper_window = f'upper_{bt.upper_window}_{bt.factor_high}'
    lower_window = f'lower_{bt.lower_window}_{bt.factor_low}'
    df = ema_crosses(df, lower_window, upper_window)
    df = df[(df['sell_signal'] == 1) | (df['buy_signal'] == 1)]
    result = run_backtest(df, result_type, bt)
    return(result)

def parallelized_backtest_v2(df, bt, my_data):
    """
    global total_left
    global time_last_start
    global next_print
    total_left -= 1
    #estimate how much time left
    
    if (next_print >= total_left):
        #seconds_left = ((time.process_time() - time_last_start) / (totalfl - total_left)) * total_left
        #print('             ', total_left, 'sets remain and', round(seconds_left / 60 , 3), 'minutes left...', end='\r')
        time_elapsed = time.time() - time_last_start
        items_calculated = int(totalfl - total_left)
        time_per_set = time_elapsed / items_calculated
        sstr = '['+str(total_left)+'/'+str(totalfl)+'] time_elapsed='+str(round(time_elapsed,3))+' items_calculated='+str(items_calculated)+' seconds per set='+str(round(time_per_set, 4))+' estimate minutes left='+str(round((time_per_set*total_left) / 60 , 3))
        print(sstr, end='\r')
        next_print -= 1
    """ 
    bt.upper_window = my_data[0]
    bt.lower_window = my_data[1]
    bt.factor_high = my_data[2]
    bt.factor_low = my_data[3]
    bt.buy_pct_usd = my_data[4]
    bt.sell_pct_btc = my_data[5]
    # trim results and ignore fills below on our start date:
    upper_window = f'upper_{bt.upper_window}_{bt.factor_high}'
    lower_window = f'lower_{bt.lower_window}_{bt.factor_low}'
    df = ema_crosses(df, lower_window, upper_window)
    df = df[(df['sell_signal'] == 1) | (df['buy_signal'] == 1)]
    result = run_backtest_reduced(df, bt)
    return(result)

def fills_running_bal(fills_df, bt):
    if len(fills_df) > 0:
        fills_df['p_usd'] = bt.principle_usd
        fills_df['p_btc'] = bt.principle_btc
        fills_df['btc_val'] = np.where(fills_df['side'] == 'sell',
                fills_df['btc_val'] * -1, fills_df['btc_val'])
        fills_df['usd_val'] = np.where(fills_df['side'] == 'buy',
                fills_df['usd_val'] * -1, fills_df['usd_val'])
        fills_df['bal_btc'] = fills_df.btc_val.cumsum() + bt.principle_btc
        fills_df['bal_usd'] = fills_df.usd_val.cumsum() + bt.principle_usd
#        fills_df['running_bal'] = (fills_df['bal_btc'] * fills_df['price']) + fills_df['bal_usd']
    else:
        logger.warning(f'Fills DataFrame is empty')
    return(fills_df)


def create_drawdowns(equity_curve):
    """
    Calculate the largest peak-to-trough drawdown of the PnL curve
    as well as the duration of the drawdown. Requires that the 
    pnl_returns is a pandas Series.

    Parameters:
    pnl - A pandas Series representing period percentage returns.

    Returns:
    drawdown, duration - Highest peak-to-trough drawdown and duration.
    """

    # Calculate the cumulative returns curve 
    # and set up the High Water Mark
    # Then create the drawdown and duration series
    hwm = [0]
    eq_idx = equity_curve.index
    drawdown = pd.Series(index = eq_idx)
    duration = pd.Series(index = eq_idx)
    df_dd = pd.DataFrame(drawdown)

    # Loop over the index range
    for t in range(1, len(eq_idx)):
        cur_hwm = max(hwm[t-1], equity_curve[t])
        hwm.append(cur_hwm)
        drawdown[t]= hwm[t] - equity_curve[t]
        duration[t]= 0 if drawdown[t] == 0 else duration[t-1] + 1
    
    return drawdown.max(), duration.max(), (drawdown.max() / equity_curve.max())