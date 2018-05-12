from datetime import datetime, timedelta
from multiprocessing import Pool
from functools import partial
from sheety import tosheet
import backtest_ema2
import pandas as pd
import itertools
import time

sheet_name = 'BacktestResults'

def backtest_set(raw_data, start_date, end_date):
    day_df, df = backtest_ema2.prep_data(raw_data, start_date, end_date)
    df = df[['close', 'time']]
    
    bt_vars = backtest_ema2.BacktestSettings()
    bt_vars.set_min_btc(.001)
    bt_vars.set_min_usd(100)
    bt_vars.set_principle_usd(25000)
    
    sell_pcts = [(2 ** x) / 1000 for x in range(3, 10)]
    buy_pcts = [(2 ** x) / 1000 for x in range(3, 10)]
    upper_windows = [(2 ** x) for x in range(4, 13)]
    lower_windows = [(2 ** x) for x in range(4, 13)]
    lower_factor_pcts = [(2 ** x) / 10000 for x in range(5, 10)]
    upper_factor_pcts = [(2 ** x) / 10000 for x in range(5, 10)]
    
    my_result_type = 'backtest'
    start = time.time()
    rois = []
    percent_iteratable = itertools.product(lower_factor_pcts, upper_factor_pcts, sell_pcts, sell_amts, lower_windows, upper_windows)
    with Pool(8) as p:
        my_partial_func = partial(backtest_ema2.run_multi, df, my_result_type, bt_vars)
        result_list = p.map(my_partial_func, percent_iteratable)
        rois.append(result_list)
    end = time.time()
    print(datetime.now(), ((end - start) / 60), "minutes")
    rois_df = pd.concat([pd.DataFrame(d) for d in rois]).reset_index(drop = True)
    rois_df = add_vectorized_cols(rois_df, bt_vars)
    return(rois_df)

def add_vectorized_cols(rois_df, bt_vars):
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

set_window = 150
set_gap = 30
start_date = '2017-11-01'
sd = datetime.strptime(start_date, '%Y-%m-%d')
ed = sd + timedelta(days = set_window)
end_date = ed.strftime('%Y-%m-%d')
result_sets = []
while ed <= datetime.strptime('2018-05-05', '%Y-%m-%d'):
    start_date = sd.strftime('%Y-%m-%d')
    end_date = ed.strftime('%Y-%m-%d')
    rois = backtest_set(raw_data, start_date, end_date)
    rois.to_csv(f'~/backfire/data/ema_{start_date}_{end_date}.csv', index = False)
    result_sets.append(rois)
    sd = sd + timedelta(days = set_gap)
    ed = ed + timedelta(days = set_gap)

all_sets = pd.concat(result_sets)
all_sets.to_csv(f'~/backfire/data/ema_all_t5m_sets_{start_date}_{end_date}.csv', index = False)

# Drop top1k into GS
top_1k_subset = all_sets.sort_values('roi', ascending = False).head(1000)
tosheet.insert_df(top_1k_subset, sheet_name, 'top1k_year', 0)

