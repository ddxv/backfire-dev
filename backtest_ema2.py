import pandas as pd
import numpy as np
from copy import deepcopy

class AccountBalances:
    usd = 0
    btc = 0
    def sub_btc(self, btc_amt):
        self.btc -= btc_amt
    def sub_usd(self, usd_amt):
        self.usd -= usd_amt
    def add_btc(self, btc_amt):
        self.btc += btc_amt
    def add_usd(self, usd_amt):
        self.usd += usd_amt
    def set_usd(self, new_bal):
        self.usd = new_bal
    def set_btc(self, new_bal):
        self.btc = new_bal

class BacktestSettings:
    upper_window = 0
    def set_upper_window(self, val):
        self.upper_window = val
    lower_window = 0
    def set_lower_window(self, val):
        self.lower_window = val
    factor_high = 0
    def set_factor_high(self, val):
        self.factor_high = val
    factor_low = 0
    def set_factor_low(self, val):
        self.factor_low = val
    buy_amt_usd = 0
    def set_buy_amt_usd(self, val):
        self.buy_amt_usd = val
    sell_pct_btc = 0
    def set_sell_pct_btc(self, val):
        self.sell_pct_btc = val
    principle_usd = 0
    def set_principle_usd(self, val):
        self.principle_usd = val
    min_usd = 0
    def set_min_usd(self, val):
        self.min_usd = val
    min_btc = 0
    def set_min_btc(self, val):
        self.min_btc = val


def run_backtest(df, desired_outputs, bt):
    bal = AccountBalances()
    bal.set_usd(bt.principle_usd)
    fills = []
    for row in list(zip(df['time'], df['high'], df['low'], df['buy_signal'], df['sell_signal'])):
        if row[3] == 1 and bal.usd > bt.min_usd:
            value_usd = bt.buy_amt_usd
            value_btc = (value_usd / row[1])
            bal.sub_usd(value_usd)
            bal.add_btc(value_btc)
            fills.append(create_fill(row[0], 'buy', row[1], value_btc, value_usd))
        if row[4] == 1 and bal.btc > bt.min_btc:
            value_btc = bal.btc * bt.sell_pct_btc
            value_usd = row[2] * bt.sell_pct_btc
            bal.add_usd(value_usd)
            bal.sub_btc(value_btc)
            fills.append(create_fill(row[0], 'sell', row[2], value_btc, value_usd))
    num_fills = len(fills)
    p_btc = bt.principle_usd / df[0:1]['close'].values[0]
    p_usd = p_btc * df.tail(1)[0:1]['close'].values[0]
    hodl_roi = (p_usd - bt.principle_usd) / bt.principle_usd
    total_roi = (bal.usd - bt.principle_usd) / bt.principle_usd
    uplift = (total_roi - hodl_roi) / hodl_roi
    result = {"sd": df[0:1]['time'].values[0], "ed": df.tail(1)[0:1]['time'].values[0],
            "roi": total_roi,
            "usd_bal": bal.usd, "fills": num_fills,
            "hodl_roi": hodl_roi, "uplift": uplift
            }
    if desired_outputs == "both":
        fills = pd.DataFrame(fills)
        return result, fills
    else:
        return result


def create_fill(my_time, my_side, btc_price, btc_val, usd_val):
    result = {}
    result['time'] = my_time
    result['side'] = my_side
    result['price'] = btc_price
    result['btc_val'] = btc_val
    result['usd_val'] = usd_val
    return(result)


def prep_data(raw_data, start_time, end_time):
    #raw_data = pd.read_csv('~/coinbase_data.csv')
    raw_data['time'] = pd.to_datetime(raw_data.time)
    trimmed_df = raw_data[raw_data.time >= start_time]
    trimmed_df = trimmed_df[trimmed_df.time <= end_time]
    trimmed_df = trimmed_df.reset_index(drop = True)
    day_df = trimmed_df.groupby(trimmed_df['time'].dt.date).agg({'open':'first',  'high': max, 'low': min, 'close':'last',}).reset_index()
    return(day_df, trimmed_df)


def single_backtest(df, bt):
    # When adjust is False, weighted averages are calculated recursively as:
    # weighted_average[i] =  alpha * arg[i] + (1 - alpha) * weighted_average[i - 1]
    upper_alpha = 2 / (bt.upper_window + 1)
    lower_alpha = 2 / (bt.lower_window + 1)
    df['upper_ema'] = (df.close * bt.factor_high).ewm(alpha = upper_alpha, adjust = False).mean()
    df['lower_ema'] = (df.close * bt.factor_low).ewm(alpha = lower_alpha, adjust = False).mean()
    df['buy_signal'] = np.where(((df.close < df.lower_ema) & (df.close.shift(1) > df.lower_ema.shift(1))), 1, 0)
    df['sell_signal'] = np.where(((df.close > df.upper_ema) & (df.close.shift(1) < df.upper_ema.shift(1))), 1, 0)
    my_results, my_fills = run_backtest(df, 'both', bt)
    return(df, my_results, my_fills)


