from datetime import datetime, timedelta
from multiprocessing import Pool
from functools import partial
import backtest_ema2
import pandas as pd
import itertools
import time


def backtest_set(raw_data, start_date, end_date):
    day_df, df = backtest_ema2.prep_data(raw_data, start_date, end_date)
    df = df[['close', 'time']]
    
    principle = 500
    principle_split = principle / 2
    principle_usd = principle_split
    principle_btc = principle_split / df[0:1]['close'].values[0]
    
    bt_vars = backtest_ema2.BacktestSettings()
    bt_vars.set_min_btc(.001)
    bt_vars.set_min_usd(100)
    bt_vars.set_principle_usd(principle_usd)
    bt_vars.set_principle_btc(principle_btc)
    bt_vars.set_start_date(start_date)
    bt_vars.set_end_date(end_date) 

    pcts = [(2 ** x) / 1000 for x in range(4, 11)]
    pcts = [1 if x > 1 else x for x in pcts]
    sell_pcts = pcts
    buy_pcts = pcts

    windows = [(2 ** x) for x in range(4, 13)]
    upper_windows = windows
    lower_windows = windows

    factor_pcts = [(2 ** x) / 10000 for x in range(5, 10)]
    lower_factor_pcts = factor_pcts
    upper_factor_pcts = factor_pcts
    
    # fix the start time for ema to unfold
    ema_length = int(pd.DataFrame(windows).max()) #find the biggest window size
    start_time_fixed = ema.get_start_time_for_ema(ema_length, start_date)

    # prep data
    day_df, df = ema.prep_data(raw_data, start_time_fixed, end_date)
    df = df[['close', 'timestamp']]

    my_result_type = 'backtest'
    start = time.time()
    rois = []
    percent_iteratable = itertools.product(lower_factor_pcts, upper_factor_pcts, sell_pcts, buy_pcts, lower_windows, upper_windows)
    with Pool(8) as p:
        my_partial_func = partial(backtest_ema2.run_multi, df, my_result_type, bt_vars)
        result_list = p.map(my_partial_func, percent_iteratable)
        rois.append(result_list)
    end = time.time()
    print(datetime.now(), ((end - start) / 60), "minutes")
    rois_df = pd.concat([pd.DataFrame(d) for d in rois]).reset_index(drop = True)
    rois_df = add_vectorized_cols(df, rois_df, bt_vars, start_date, end_date)
    return(rois_df)


def add_vectorized_cols(df, rois_df, bt_vars, start_date, end_date):
    rois_df['sd'] = start_date
    rois_df['ed'] = end_date
    final_close = df.tail(2)[0:1]['close'].values[0]
    hodl_usd = (bt_vars.principle_usd / df[0:1]['close'].values[0]) * final_close
    hodl_roi = (hodl_usd - bt_vars.principle_usd) / bt_vars.principle_usd
    rois_df['hodl_roi'] = hodl_roi
    rois_df['final_bal'] = rois_df['usd_bal'] + (final_close * rois_df['btc_bal'])
    rois_df['roi'] = (rois_df['final_bal'] - bt_vars.principle_usd) / bt_vars.principle_usd
    return(rois_df)


raw_data = pd.read_csv('~/backfire/data/coinbase_fixed_2014-12-01_2018-05-06.csv')

test_name = 'stairstepsfive_gdax'
first_date = '2017-12-01'
set_window = 90
set_step = 5
sd = datetime.strptime(first_date, '%Y-%m-%d')
ed = sd + timedelta(days = set_window)
end_date = ed.strftime('%Y-%m-%d')
result_sets = []
while ed <= datetime.strptime('2018-04-05', '%Y-%m-%d'):
    start_date = sd.strftime('%Y-%m-%d')
    end_date = ed.strftime('%Y-%m-%d')
    print(sd, ed)
    rois = backtest_set(raw_data, start_date, end_date)
    #rois.to_csv(f'~/backfire/data/{test_name}/ema_{start_date}_{end_date}.csv', index = False)
    result_sets.append(rois)
    sd = sd + timedelta(days = set_step + 1)
    ed = sd + timedelta(days = set_window)
all_sets = pd.concat(result_sets)

folder_name = f'~/backfire/data/{test_name}/ema_all_{set_window}w_{set_step}s_{first_date}.csv'
print(folder_name)
all_sets.to_csv(folder_name, index = False)


