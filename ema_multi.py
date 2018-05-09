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
    bt_vars.set_principle_usd(25000)
    
    sell_pcts = [(2 ** x) / 1000 for x in range(3, 10)]
    sell_amts = [(2 ** x) for x in range(7, 15)]
    upper_windows = [(2 ** x) for x in range(4, 13)]
    lower_windows = [(2 ** x) for x in range(4, 13)]
    lower_factor_pcts = [(2 ** x) / 10000 for x in range(5, 10)]
    upper_factor_pcts = [(2 ** x) / 1000 for x in range(4, 10)]

    #upper_windows = [1, 2]
    #lower_windows = [1, 2]
    #sell_amts = [1, 2]

    start = time.time()
    my_result_type = 'backtest'
    rois = []
    percent_iteratable = itertools.product(lower_factor_pcts, upper_factor_pcts, sell_pcts, sell_amts, lower_windows, upper_windows)
    with Pool(8) as p:
        my_partial_func = partial(backtest_ema2.run_multi, df, my_result_type, bt_vars)
        result_list = p.map(my_partial_func, percent_iteratable)
        rois.append(result_list)
    end = time.time()
    print(((end - start) / 60))
    
    rois_df = pd.concat([pd.DataFrame(d) for d in rois]).reset_index(drop=True)
    rois_df['sd'] = start_date
    rois_df['ed'] = end_date
    final_close = df.tail(2)[0:1]['close'].values[0]
    hodl_usd = (bt_vars.principle_usd / df[0:1]['close'].values[0]) * final_close
    hodl_roi = (hodl_usd - bt_vars.principle_usd) / bt_vars.principle_usd
    rois_df['hodl_roi'] = hodl_roi
    rois_df['final_bal'] = rois_df['usd_bal'] + (final_close * rois_df['btc_bal'])
    rois_df['roi'] = (rois_df['final_bal'] - bt_vars.principle_usd) / bt_vars.principle_usd
    return(rois_df)


#test_rois = backtest_set(raw_data, start_date, end_date)


raw_data = pd.read_csv('~/backfire/data/coinbase_2014-12-01_2018-05-06.csv')

start_date = '2017-01-01'
end_date = '2018-05-06'

first_date = datetime.strptime(start_date, '%Y-%m-%d')
sd = first_date
result_sets = []
while sd < (first_date + timedelta(20)):
    sd = sd + timedelta(days = 5)
    print(sd)
    start_date = sd.strftime('%Y-%m-%d')
    rois = backtest_set(raw_data, start_date, end_date)
    rois.to_csv(f'~/backfire/data/ema_set_{start_date}_2018-05-06.csv', index=False)
    result_sets.append(rois)
all_sets = pd.concat(result_sets)
all_sets.to_csv(f'~/backfire/data/ema_five_set_{start_date}_2018-05-06.csv', index=False)


top_1k_subset = all_sets.sort_values('roi', ascending = False).head(1000)
tosheet.insert_df(top_1k_subset, sheet_name, 'top1k_year', 0)

positive_roi_subset = all_sets[all_sets['roi'] > 0]


# Feature Extraction with Univariate Statistical Tests (Chi-squared for classification)
from sklearn.feature_selection import SelectKBest, SelectPercentile
from sklearn.feature_selection import mutual_info_regression
import numpy as np

feature_names = ['upper_window', 'lower_window', 'lower_factor', 'upper_factor', 'buy_amt_usd', 'sell_pct_btc']

testdf = all_sets.copy()
arrayY = testdf.roi.values
arrayX = testdf[feature_names].values

# feature extraction
k_num = 3
#test = SelectPercentile(score_func = partial(mutual_info_regression, discrete_features = True))
test = SelectKBest(score_func = partial(mutual_info_regression, discrete_features = True), k = k_num)
fit = test.fit(arrayX, arrayY, )

kbest_scores = pd.DataFrame([list(fit.scores_)], columns = feature_names)
print(kbest_scores)

# Look up what this was for, it appears to 'select' only Kbest features
features = fit.transform(arrayX)

print(features[0:5,:])
np.set_printoptions(suppress=True)


kbest_cols = list(kbest_scores.columns[np.argsort(list(kbest_scores))])[:k_num]
kbest_means = all_sets.groupby(kbest_cols)['usd_bal', 'roi', 'n_fills'].mean().reset_index().sort_values('roi', ascending = False).head(100)
kbest_means


tosheet.insert_df(kbest_means, sheet_name, 'kbest_means', 0)

tosheet.insert_df(kbest_scores, sheet_name, 'kbest_scores', 0)




minute_df = pd.read_csv('~/backfire/data/coinbase_2014-12-01_2018-05-06.csv')

minute_df = minute_df[minute_df.close != .06].reset_index(drop=True)

minute_df.to_csv('~/backfire/data/coinbase_fixed_2014-12-01_2018-05-06.csv', index=False)




