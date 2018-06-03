#imports
from datetime import datetime, timedelta
from multiprocessing import Pool
from functools import partial
from backfire import ema
import pandas as pd
import itertools
import time

# runs the entiry backtets for this set of date with all the possible combinations
def backtest_set(raw_data, start_date, end_date):
    principle = 35000
    principle_split = principle / 2
    principle_usd = principle # principle_split
    principle_btc = 0 # principle_split / df[0:1]['close'].values[0]
    
    bt_vars = ema.BacktestSettings()
    bt_vars.set_min_btc(.001)
    bt_vars.set_min_usd(100)
    bt_vars.set_principle_usd(principle_usd)
    bt_vars.set_principle_btc(principle_btc)
    
#    pcts = [0.01, 0.05, 0.2, 0.4, 0.5, 0.6, 0.8, 1.0]
    pcts = [0.5, 1.0]
    sell_pcts = pcts
    #sell_pcts = [0.1]
    #buy_pcts = [0.1]
    buy_pcts = pcts

#    windows = [10,20,40,60,80,100,140,160,200,300,400,500,600,700,800,1000,2000]
#    windows = [20,40,60,80,100,150,200,300,400,500,600,700,800]
    windows = [4,8,12,16,20,25,30,40,50,60,70,80,90,100,200,400,600,1000]
    upper_windows = windows
    lower_windows = windows

#    factor_pcts = [0.002, 0.004, 0.008, 0.01, 0.015, 0.02, 0.025, 0.03, 0.035, 0.04, 0.045, 0.05, 0.06]
    factor_pcts = [0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.012, 0.014, 0.016, 0.018, 0.02, 0.022]
    lower_factor_pcts = factor_pcts
    upper_factor_pcts = factor_pcts
    
    threshold_roi = 0.1
    threshold_hodl_diff = 0.1
    threshold_fills = 5
    
    # fix the start time for ema to unfold
    ema_length = int(pd.DataFrame(windows).max()) #find the biggest window size
    start_time_fixed = ema.get_start_time_for_ema(ema_length, start_date)

    # prep data
    day_df, df = ema.prep_data(raw_data, start_time_fixed, end_date)
    df = df[['close', 'timestamp']]

    my_result_type = 'backtest'
    start = time.time()
    rois = []
    # this makes a product matrix of all possible combinations:
    percent_iteratable = itertools.product(
        upper_windows, 
        lower_windows,
        upper_factor_pcts,
        lower_factor_pcts, 
        buy_pcts, 
        sell_pcts)
    with Pool(4) as p: #how many threads are being used
        my_partial_func = partial(ema.run_multi, df, my_result_type, bt_vars) # this runs the actual backtest
        result_list = p.map(my_partial_func, percent_iteratable)
        rois.append(result_list)
        # filter: Can't filter here, result_list is always the complete thing at once
    end = time.time()
    print(datetime.now(), ((end - start) / 60), "minutes")
    rois_df = pd.concat([pd.DataFrame(d) for d in rois]).reset_index(drop = True)
    rois_df = add_vectorized_cols(df, rois_df, bt_vars, start_date, end_date)

    # filter somewhere here and create a new pd out of it.
    return(rois_df)

 # adds the columsn that are vectorized
def add_vectorized_cols(df, rois_df, bt_vars, start_date, end_date):
    #rois_df['sd'] = start_date
    #rois_df['ed'] = end_date
    final_close = df.tail(2)[0:1]['close'].values[0]
    hodl_usd = (bt_vars.principle_usd / df[0:1]['close'].values[0]) * final_close
    hodl_roi = (hodl_usd - bt_vars.principle_usd) / bt_vars.principle_usd
    rois_df['hodl_roi'] = hodl_roi
    rois_df['final_bal'] = rois_df['usd_bal'] + (final_close * rois_df['btc_bal'])
    rois_df['roi'] = (rois_df['final_bal'] - bt_vars.principle_usd) / bt_vars.principle_usd
    return(rois_df)



#raw_data = pd.read_csv('~/backfire/data/resources/coinbase_fixed_2017-01-01_current.csv')
raw_data = pd.read_csv('~/backfire/data/resources/Bitfinex_BTCUSD_1min_2017-11-20_to_2018-06-03.csv')
#raw_data = pd.read_csv('~/backfire/data/resources/Bitfinex_ETHUSD_1min_2017-12-14_to_2018-05-30.csv')

test_name = 'tobi_BTC_bitfinex_3_days'
start_date = "2018-06-01"
end_date = "2018-06-03"
result_sets = []

rois = backtest_set(raw_data, start_date, end_date)
#rois.to_csv(f'~/backfire/data/{test_name}/ema_{start_date}_{end_date}.csv', index = False)
result_sets.append(rois)
all_sets = pd.concat(result_sets)

folder_name = f'~/backfire/data/{test_name}_ematest_{start_date}_{end_date}.csv'
print(folder_name)
all_sets.to_csv(folder_name, index = False)