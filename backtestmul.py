from multiprocessing import Pool
import pandas as pd
import itertools
import pandas as pd
import json
from datetime import datetime, timedelta
from functools import partial
import pandas as pd
import time
from time import sleep
#import mysql_prices as ms
from copy import deepcopy
import backtest
import numpy as np

#start_time = datetime.now().replace(microsecond = 0) - timedelta(days = 110)
#end_time = datetime.now().replace(microsecond = 0) - timedelta(days = 96)

start_time = '2016-08-31'
end_time = '2017-11-17'
sdf = pd.read_csv("~/coinbase_data.csv")

sdf_time = pd.to_datetime(sdf.time)
sdf = sdf[sdf_time >= start_time ]
sdf = sdf[sdf_time <= end_time ]

my_percents = [(2 ** x) / 10000 for x in range(0, 10)]
start = time.time()
my_result_type = 'backtest'
rois = []
bw = 2
sw = 1
while bw <= 586:
    sw = 1
    sdf['bw'] = sdf.rolling(window = bw, min_periods = bw)['mean'].mean()
    while sw <= bw/2:
        sdf['sw'] = sdf.rolling(window = sw, min_periods = sw)['mean'].mean()
        sdf['p'] = (sdf['sw'] / sdf['bw']) - 1
        percent_iteratable = itertools.product(my_percents, my_percents, my_percents)
        with Pool(8) as p:
            my_partial_func = partial(backtests.run_backtest, sdf, bw, sw, my_result_type)
            result_list = p.map(my_partial_func, percent_iteratable)
            rois.append(result_list)
        sw = sw * 2
    print(bw)
    bw = bw * 2
end = time.time()
print(((end - start) / 60))


df = pd.concat([pd.DataFrame(d) for d in rois]).reset_index(drop=True)
principal = 100
p_btc = principal / sdf[0:1]['mean'].values[0]
hodl_usd = p_btc * sdf.tail(1)[0:1]['mean'].values[0]
hodl_roi = (hodl_usd - principal) / principal
df['hodl_roi'] = hodl_roi
total_roi = (usdVal - principal) / principal
#uplift = (total_roi - hodl_roi) / hodl_roi


df['st'] = start_time
df['et'] = end_time

df.to_csv("backtests/{}_{}.csv".format(start_time, end_time), index = False)


dflow = pd.read_csv("~/backtests/2017-08-31 21:04:07_2017-09-14 21:04:07.csv")



# Feature Extraction with Univariate Statistical Tests (Chi-squared for classification)
import pandas
import numpy
from sklearn.feature_selection import SelectKBest, SelectPercentile
from sklearn.feature_selection import mutual_info_regression

testdf = df
arrayY = testdf.roi.values
arrayX = testdf[['bw', 'sw', 'pb', 'ps', 'x']].values

# feature extraction
#test = SelectPercentile(score_func = partial(mutual_info_regression, discrete_features = True))
test = SelectKBest(score_func = partial(mutual_info_regression, discrete_features = True), k = 3)
fit = test.fit(arrayX, arrayY, )
print(fit.scores_)

features = fit.transform(arrayX)
print(features[0:5,:])

np.set_printoptions(suppress=True)

fit.scores_

df[df.fills>10].groupby(['pb', 'ps', 'x'])['rev', 'roi', 'fills'].mean().reset_index().sort_values('roi', ascending = False).head(5)


df[df.fills>600].sort_values('roi', ascending = False).head()[['bw', 'sw', 'pb', 'ps', 'x', 'roi','fills', 'hodl_roi']]


