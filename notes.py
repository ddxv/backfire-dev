import pandas as pd
# Feature Extraction with Univariate Statistical Tests (Chi-squared for classification)
from sklearn.feature_selection import SelectKBest, SelectPercentile
from sklearn.feature_selection import mutual_info_regression
import numpy as np
from functools import partial


feature_names = ['upper_window', 'lower_window', 'lower_factor', 'upper_factor', 'buy_amt_usd', 'sell_pct_btc']

five_df = pd.read_csv('~/backfire/data/ema_all_5m_sets_2017-11-28_2018-05-05.csv')
two_df = pd.read_csv('~/backfire/data/ema_all_2m_sets_2018-02-26_2018-05-04.csv')
year_df = pd.read_csv('~/backfire/data/spring/wholeyear/ema_five_set_2017-01-16_2018-05-06.csv')

all_df = pd.concat([year_df])
grouped_df = all_df.groupby(feature_names)['roi', 'hodl_roi'].mean().reset_index()
vars_df =  []
for f in feature_names:
    f_df = grouped_df.groupby(f)['roi', 'hodl_roi'].mean().reset_index()
    f_df['feature'] = f
    f_df = f_df.rename(columns = {f: 'vars'})
    vars_df.append(f_df)
vars_df = pd.concat(vars_df).reset_index(drop = True)
vars_df.sort_values('roi', ascending = True).head(10)
vars_df[vars_df['feature']=='upper_factor'].vars - 1














testdf = all_df.copy()
arrayY = testdf.roi.values
arrayX = testdf[feature_names].values

# feature extraction
k_num = 3
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



sheet_name = 'BacktestResults'
col_order_names = ['upper_factor', 'lower_factor', 'upper_window', 'lower_window', 'sell_pct_btc', 'buy_amt_usd', 'roi', 'sd', 'ed', 'usd_bal', 'btc_bal', 'final_bal', 'hodl_roi', 'n_fills']

filename = 'backfire/data/ema_5m_set_2017-10-29_2018-04-05.csv'
one_df = pd.read_csv(filename)
top_1k_subset = one_df.sort_values('roi', ascending = False).head(1000)

top_1k_subset = top_1k_subset[col_order_names]

tosheet.insert_df(top_1k_subset, sheet_name, 'top1k_year', 0)








