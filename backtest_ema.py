from multiprocessing import Pool
import pandas as pd
import itertools
import pandas as pd
import gdax
import json
from datetime import datetime, timedelta
from functools import partial
import pandas as pd
import time
from time import sleep
#Local Packages
#import mysql_prices as ms
from copy import deepcopy

#Initialize Account Values
minUSD = 66
btcAcc = None
usdAcc = None
btcVal = None
usdVal = None
btcHold = None
usdHold = None
#All Buy/Sells
ace_of_spades = False
#Sandbox
joker = False


def bt_buy(usd, time, price):
    result = {}
    result['t'] = time
    print(usd)
    btc_amt = (usd - 1) / price
    result['price_usd'] = price
    result['buy_size'] = btc_amt
    return(result)


def replace_fills(fills):
    con = ms.connect_mysql()
    fills.to_sql('backtest_fills',con, if_exists="replace", index=False)


def make_windows(df, bw, sw):
    df['bw'] = df.rolling(window = bw, min_periods = bw)['mean'].mean()
    df['sw'] = df.rolling(window = sw, min_periods = sw)['mean'].mean()
    df['p'] = (df['sw'] / df['bw']) - 1
    #df = df[24:].head(10000)
    return(df['p'])





#fills = run_backtest(alldf, pb, ps)





def run_backtest(df, my_range, factor_h, factor_l):
    principle = 100
    usdVal = principle
    minUSD = 80
    fills = []
    open_buys = []
    open_sells = []
    #All Buy/Sells
    for row in list(zip(df['time'], df['close'], df['mean'], df['b'], df['s'])):
        if len(open_buys) == 0:
            if row[3] == 1 and usdVal > 1:
                new_order = bt_buy(usdVal, row[0], row[2])
                new_order['side'] = 'buy'
                usdVal = usdVal - (new_order['price_usd'] * new_order['buy_size'])
                print(usdVal)
                fills.append(new_order)
                open_buys.append(new_order)
                continue
            continue
        elif len(open_buys) > 0:
            if row[4] == 1:
                my_buy = open_buys[0]
                open_buys = []
                usdVal = usdVal + (row[2] * my_buy['buy_size'])
                print(usdVal)
                my_sell = deepcopy(my_buy)
                my_sell['t'] = row[0]
                my_sell['price_usd'] = row[2]
                my_sell['side'] = 'sell'
                fills.append(my_sell)
                continue
    #num_sells
    p_btc = principle / df[0:1]['mean'].values[0]
    p_usd = p_btc * df.tail(1)[0:1]['mean'].values[0]
    hodl_roi = (p_usd - principle) / principle
    total_roi = (usdVal - principle) / principle
    uplift = (total_roi - hodl_roi) / hodl_roi
    result = {"range": my_range, "factor_h": factor_h, "factor_l": factor_l, "roi": total_roi, "hodl_roi": hodl_roi, "uplift": uplift}
    return result, fills



def parallel_runs(data_list, sdf, bw, sw):
    with Pool(8) as p:
        my_partial_func = partial(run_backtest, sdf, bw, sw)
        result_list = p.map(my_partial_func, data_list)
    return result_list


my_percents = [(2 ** x) / 10000 for x in range(0, 10)]
my_bw = [(2 ** x) for x in range(1, 11)]
my_sw = [(2 ** x) for x in range(0, 10)]


"""// the actual exponential moving average
sma1 = ema(c1 * (1.0 + factor1), range1)
sma2 = ema(c1 * (1 - factor2), range1)

ema
The sma function returns the exponentially weighted moving average. In ema weighting factors decrease exponentially. It calculates by sing a formula: EMA = alpha * x + (1 - alpha) * EMA[1], where alpha = 2 / (y + 1)
ema(source, length) → series
EXAMPLE
plot(ema(close, 15))

"""



startTime = datetime.now().replace(microsecond = 0) - timedelta(days = 365)
endTime = datetime.now().replace(microsecond = 0) - timedelta(days = 1)
#alldf = ms.get_btc_prices_usd_minute(startTime)
#alldf.to_csv("~/alldf.csv", index = False)
sdf = pd.read_csv("~/coinbase_data.csv")
sdf.time = pd.to_datetime(sdf.time)
sdf = sdf[sdf.time > startTime ]
sdf = sdf[sdf.time < endTime ]
start = time.time()


rois = []
my_range = 1000
factor_high = 0.04
factor_low = 0.03

factor_high = 1 + factor_high
factor_low = 1 - factor_low

sdf['h2'] = (sdf['high'] + sdf['low']) / 2

sdf['h'] = (sdf.h2 * factor_high).ewm(span = my_range).mean()
sdf['l'] = (sdf.h2 * factor_low).ewm(span = my_range).mean()
# EMA [today] = (Price [today] x K) + (EMA [yesterday] x (1 – K))

my_alpha = 2 / (my_range + 1)
#sdf['h'] = ((sdf.h2 * factor_high) * my_alpha) + ((sdf.h2.shift(1) * factor_high) * (1 - my_alpha))
#sdf['l'] = ((sdf.h2 * factor_low) * my_alpha) + ((sdf.h2.shift(1) * factor_low) * (1 - my_alpha))



sdf['s'] = np.where(((sdf.h2 < sdf.h) & (sdf.h2 > sdf.h.shift(1))), 1, 0)
max_sell_time = sdf[sdf.s == 1].time.max()
sdf['b'] = np.where(((sdf.h2 > sdf.l) & (sdf.h2 < sdf.l.shift(1)) & (sdf.time < max_sell_time)), 1, 0)


results, my_fills = run_backtest(sdf, my_range, factor_high, factor_low)
my_fills = pd.DataFrame(my_fills)
results

my_fills.to_csv("tobifills.csv", index = False)





while bw <= 2:
    sw = 1
    sdf['bw'] = sdf.rolling(window = bw, min_periods = bw)['mean'].mean()
    while sw <= bw/2:
        sdf['sw'] = sdf.rolling(window = sw, min_periods = sw)['mean'].mean()
        sdf['p'] = (sdf['sw'] / sdf['bw']) - 1
        dis = itertools.product(my_percents, my_percents, my_percents)
        with Pool(8) as p:
            my_partial_func = partial(run_backtest, sdf, bw, sw)
            result_list = p.map(my_partial_func, dis)
            rois.append(result_list)
        sw = sw * 2
    bw = bw * 2
end = time.time()
print(((end - start) / 60))



df = pd.concat([pd.DataFrame(d) for d in rois]).reset_index(drop=True)



df['st'] = startTime
df['et'] = endTime

df.to_csv("backtests/{}_{}.csv".format(startTime, endTime), index = False)



df = pd.read_csv("~/backtests/2017-08-17 17:00:26_2017-09-22 17:00:26.csv")




# Feature Extraction with Univariate Statistical Tests (Chi-squared for classification)
import pandas
import numpy
from sklearn.feature_selection import SelectKBest, SelectPercentile
from sklearn.feature_selection import mutual_info_regression


testdf = df
arrayY = testdf.uplift.values
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


