




#LIVE ACCOUNT
ac = initialize_gdax(False)
mult = 1


#SANDBOX
ac = initialize_gdax(True)
mult = 2


pc = gdax.PublicClient()

#Initialize Account Values
usdMin = 90

keep_usd_in_account = 2

btcAcc = None
usdAcc = None
btcVal = None
usdVal = None
btcHold = None
usdHold = None
#All Buy/Sells
#Sandbox
joker = True



eng = ms.connect_mysql()
result = eng.execute("select max(mean) from btc_price_usd_minute")
for r in result:
    recent_max = r[0]
result.close()
my_max = float(recent_max) * .995
my_max


#Human readable Accounts DF
accDF = pd.DataFrame(ac.get_accounts())

##Get previously set orders, decide, may be long orders
open_orders = get_open_orders()

long_orders=[]
long_orders = open_orders['oldIds'] + open_orders['youngIds']



##Just Kill all old orders?
#stale_orders = get_open_orders()['oldIds']
#kill_orders(stale_orders)



#Drop Old Fills & Orders and Remake Tables
#ms.drop_table('test_fills')
ms.drop_table('test_orders')


orders = pd.DataFrame(ac.get_orders()[0])
if len(orders>0):
    con = ms.connect_mysql()
    orders.to_sql('test_orders',con, if_exists="replace", index=False)


#Check & Replace Fills table
fills = check_fills()


cur = 'BTC'
bw = 25
sw = 3
x = .0015
pb = .001666
ps = .000505

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
    eng = ms.connect_mysql()
    result = eng.execute("select max(mean) from btc_price_usd_minute")
    for r in result:
        recent_max = r[0]
    result.close()
    my_max = float(recent_max) * .995

