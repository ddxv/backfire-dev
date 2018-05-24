import pandas as pd
import numpy as np
#import mysql_prices as ms


class AccountBalances:
    usd_bal = 0
    btc_bal = 0
    def set_usd_bal(self, new_bal):
        self.usd_bal = new_bal
    def set_btc_bal(self, new_bal):
        self.btc_bal = new_bal

class BuyManager:
    cur_buys = 0
    stop_price = 0
    next_buy_price = 0
    def set_stop(self, sell_at):
        self.stop_price = sell_at
    def set_cur_buys(self, cur_buys):
        self.cur_buys = 0
    def set_next_buy(self, buy_at):
        self.cur_buys += 1
        self.next_buy_price = buy_at

def gather_data(data_source):
    if data_source ==  "kaggle_coinbase":
        source_df = pd.read_csv("~/coinbase_data.csv")
        return source_df
    if data_source == "mysql_gdax":
        #source_df = ms.get_gdax_prices(start_time)
        pass
    else:
        raise NameError(f'{data_source} does not exist')


#sdf = gather_data("kaggle_coinbase")
#start_time = "2017-01-01"
#end_time = "2017-10-01"


def single_backtest(start_time, end_time, data_source):
    sdf = gather_data(data_source)
    sdf['time'] = pd.to_datetime(sdf.time)
    sdf = sdf[sdf.time >= start_time]
    sdf = sdf[sdf.time <= end_time ]
    buy_sell_df, day_df = prepare_df(sdf)
    my_results, my_fills = run_backtest(buy_sell_df, "both")
    my_fills = pd.DataFrame(my_fills)
    return day_df, my_results, my_fills

def prepare_df(raw_df):
    raw_df['time'] = pd.to_datetime(raw_df.time)
    day_df = raw_df.groupby(raw_df['time'].dt.date).agg({'open':'first',  'high': max, 'low': min, 'close':'last', 'volumeto': sum, 'volumefrom': sum,}).reset_index()
    day_df['rolling_max'] = day_df.high.rolling(28, min_periods=28, center = False).max()
    day_df['rolling_min'] = day_df.low.rolling(14, min_periods=14, center = False).min()
    day_df['atr'] = pd.concat([
    abs(day_df.high - day_df.low),
    abs(day_df.close.shift(1) - day_df.high),
    abs(day_df.close.shift(1) - day_df.low),
    ], axis = 1).max(1)
    day_df['n'] = day_df.atr.rolling(20, min_periods=20).mean()
    day_df = day_df[27:]
    day_df['buy_signal'] = np.where(day_df.high > day_df.shift(1).rolling_max, 1, 0)
    day_df['sell_signal'] = np.where(day_df.low < day_df.shift(1).rolling_min, 1, 0)
    buy_sell_df = day_df[(day_df['buy_signal'] == 1) | (day_df['sell_signal'] == 1)]
    return(day_df, day_df)


def run_backtest(df, desired_result):
    usd_principle = 10000
    acc = AccountBalances()
    buyer = BuyManager()
    acc.set_usd_bal(usd_principle)
    usd_min = 50
    btc_min = .01
    fills = []
    # time=row0, high=row1, low=row2, buy=row3, sell=row4, n=row5
    for row in list(zip(df['time'], df['high'], df['low'], df['buy_signal'], df['sell_signal'], df['n'])):
        if row[3] == 1 and acc.usd_bal > usd_min and buyer.cur_buys < 5:
            usd_val = acc.usd_bal * .02
            btc_val = usd_val / row[1]
            new_usd_bal = acc.usd_bal - usd_val
            acc.set_usd_bal(new_usd_bal)
            new_btc_bal = acc.btc_bal + btc_val
            acc.set_btc_bal(new_btc_bal)
            buyer.set_stop(row[1] - (2 * row[5]))
            buyer.set_next_buy(row[1] + row[5])
            fills.append(create_fill(row[0], 'buy', row[1], btc_val, usd_val))
        if row[4] == 1 and acc.btc_bal > btc_min or buyer.stop_price > row[2]:
            usd_val = row[2] * acc.btc_bal
            btc_val =  acc.btc_bal
            new_usd_bal = acc.usd_bal + usd_val
            acc.set_usd_bal(new_usd_bal)
            acc.set_btc_bal(0)
            buyer.set_stop(0)
            buyer.set_cur_buys(0)
            fills.append(create_fill(row[0], 'sell', row[2], btc_val, usd_val))
    num_fills = len(fills)
    p_btc = usd_principle / df[0:1]['close'].values[0]
    p_usd = p_btc * df.tail(1)[0:1]['close'].values[0]
    hodl_roi = (p_usd - usd_principle) / usd_principle
    total_roi = (acc.usd_bal - usd_principle) / usd_principle
    uplift = (total_roi - hodl_roi) / hodl_roi
    result = {"sd": df[0:1]['time'].values[0], "ed": df.tail(1)[0:1]['time'].values[0],
            "roi": total_roi,
            "usd_bal": acc.usd_bal, "fills": num_fills,
            "hodl_roi": hodl_roi, "uplift": uplift
            }
    if desired_result == "both":
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


