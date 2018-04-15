import pandas as pd
from datetime import timedelta
from copy import deepcopy


def single_backtest(start_time, end_time, bw, sw, pb, ps, x):
    my_data = [pb, ps, x]
    sdf = pd.read_csv("~/coinbase_data.csv")
    sdf.time = pd.to_datetime(sdf.time)
    sdf = sdf[sdf.time >= start_time ]
    sdf = sdf[sdf.time <= end_time ]
    sdf['bw'] = sdf.rolling(window = bw, min_periods = bw)['mean'].mean()
    sdf['sw'] = sdf.rolling(window = sw, min_periods = sw)['mean'].mean()
    sdf['p'] = (sdf['sw'] / sdf['bw']) - 1
    my_results, my_fills = run_backtest(sdf, bw, sw, "both", my_data)
    my_fills = pd.DataFrame(my_fills)
    return sdf, my_results, my_fills


def bt_buy(usd, time, price, pb, ps):
    result = {}
    result['t'] = time
    lb = price
    #Margins
    buy_limit = lb - (lb * pb)
    btc_amt = (usd - (usd * .0028)) / buy_limit
    sell_bid  = lb + (lb * ps)
    result['buy_bid'] = buy_limit
    result['buy_size'] = btc_amt
    result['sell_bid'] = sell_bid
    result['sell_size'] = btc_amt
    return(result)



def run_backtest(df, bw, sw, desired_result, my_data):
    pb = my_data[0]
    ps = my_data[1]
    x = my_data[2]
    principle = 100
    usdVal = principle
    minUSD = 80
    fills = []
    open_buys = []
    open_sells = []
    #All Buy/Sells
    for row in list(zip(df['time'], df['close'], df['mean'], df['p'])):
        if len(open_buys) == 0 and len(open_sells) == 0:
            if row[3] < -x and usdVal > minUSD:
                new_order = bt_buy(usdVal, row[0], row[2], pb, ps)
                open_buys.append(new_order)
                continue
            continue
        elif len(open_buys) > 0:
            if row[0] > (open_buys[0]['t'] + timedelta(hours = 1)):
                open_buys = []
                continue
            if row[1] < open_buys[0]['buy_bid']:
                my_buy = open_buys[0]
                open_buys = []
                usdVal = usdVal - my_buy['buy_bid'] * my_buy['buy_size']
                my_sell = deepcopy(my_buy)
                open_sells.append(my_sell)
                my_buy['close_t'] = row[0]
                my_buy['side'] = 'buy'
                fills.append(my_buy)
                continue
        elif len(open_sells) > 0 and row[1] > open_sells[0]['sell_bid']:
            my_sell = open_sells[0]
            my_sell['close_t'] = row[0]
            sell_usd = my_sell['sell_bid'] * my_sell['sell_size']
            open_sells = []
            #num_sells += 1
            usdVal = sell_usd + usdVal
            my_sell['side'] = 'sell'
            fills.append(my_sell)
    if len(open_buys) > 0:
        my_buy = open_buys[0]
        open_buys = []
        usdVal = usdVal + my_buy['buy_bid'] * my_buy['buy_size']
    if len(open_sells) > 0:
        my_sell = open_sells[0]
        open_sells = 0
        usdVal = usdVal + my_sell['sell_size'] * df.tail(1)[0:1]['mean'].values[0]
    num_sells = len(fills)
    total_roi = (usdVal - principle) / principle
    result = {"bw": bw, "sw": sw,
            "pb": pb, "ps": ps, "x": x,
            "roi": total_roi,
            "rev": usdVal, "fills": num_sells}
    if desired_result == "both":
        return result, fills
    else:
        return result






