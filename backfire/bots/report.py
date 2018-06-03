import gdax
import json
from datetime import datetime, timedelta
import pandas as pd
from time import sleep

#Local Packages
import mysql_prices as ms

#SB = SandBox
#gdax_auth = json.load(open('/home/bitnami/auth/gdax_sb'))
gdax_auth = json.load(open('/home/bitnami/auth/gdax'))
secret=gdax_auth['secret']
key=gdax_auth['key']
passphrase=gdax_auth['passphrase']

# Use the sandbox API (requires a different set of API access credentials)
#ac = gdax.AuthenticatedClient(key, secret, passphrase, api_url="https://api-public.sandbox.gdax.com")
ac = gdax.AuthenticatedClient(key, secret, passphrase)
public_client = gdax.PublicClient()

fills = pd.DataFrame(ac.get_fills()[0])
fills.created_at=pd.to_datetime(fills.created_at)

my_orders=ac.get_orders()[0]

my_orders = pd.DataFrame(my_orders)




openOrders = myOrders[myOrders['status'] == 'open']
        oldIds = openOrders[pd.to_datetime(openOrders['created_at']) < datetime.now() - timedelta(minutes=20)]['id'].tolist()
        youngIds = openOrders[pd.to_datetime(openOrders['created_at']) >= datetime.now() - timedelta(minutes=20)]['id'].tolist()
    myIds = {"youngIds": youngIds, "oldIds": oldIds}
    return(myIds)




