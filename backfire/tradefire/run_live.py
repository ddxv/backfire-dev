from backfire.backtest.ema import AccountBalances, AlgSettings



#LIVE ACCOUNT
#ac = initialize_gdax(False)
#mult = 1


#SANDBOX
ac = initialize_gdax(True)
mult = 2


pc = gdax.PublicClient()


# Human readable Accounts DF
account_df = pd.DataFrame(ac.get_accounts())

# Get previously set orders, decide, may be long orders
open_orders = get_open_orders()


alg_vars = AlgSettings()

#Initialize Account Values

alg_vars.set_principle_usd(40)
alg_vars.set_principle_btc(.04)
alg_vars.set_upper_window(64)
alg_vars.set_lower_window(64)
upper_factor = 1.0128
lower_factor = 0.9744
alg_vars.set_factor_high(upper_factor)
alg_vars.set_factor_low(lower_factor)
alg_vars.set_buy_pct_usd(0.5)
alg_vars.set_sell_pct_btc(0.12)
alg_vars.set_min_usd(100)
alg_vars.set_min_btc(.001)

#Start Looping Buy orders & Sell Orders
while True:
    update_account_info()
    while usdBal < usdMin:
        logger.info("USD balance less than minimum")
        sleep(300)
        update_account_info()
    open_orders = get_open_orders()
    open_order_ids = open_orders['youngIds'] + open_orders['oldIds']
    if len(open_order_ids) < 2:
        print("starting dip check loop")
        tmp = 0
        new_order = dip_check_loop(bw, sw, pb, ps, x, tmp, cur)
        if new_order is not None:
            result = fill_loop(new_order, cur)
    fills = check_fills()
    sleep(180)

