from datetime import datetime
from os import system
import pandas as pd
import json

def merge_mysql_csv():
    mysql_gdax = pd.read_csv('/home/bitnami/backfire/data/resources/gdax_mysql.csv')
    most_recent_date = datetime.strptime(mysql_gdax.time.max(), '%Y-%m-%d %H:%M:%S').date()
    mysql_gdax = mysql_gdax[pd.to_datetime(mysql_gdax['time']) < most_recent_date]
    old_gdax = pd.read_csv('/home/bitnami/backfire/data/resources/coinbase_fixed_2017-01-01_current.csv')
    old_gdax = pd.concat([old_gdax, mysql_gdax])
    old_gdax = old_gdax.drop_duplicates().reset_index(drop = True)
    old_gdax.to_csv('/home/bitnami/backfire/data/resources/coinbase_fixed_2017-01-01_current.csv', index = False)


if __name__ == '__main__':
    system('/home/bitnami/auth/dl_gdax.sh')
    merge_mysql_csv()

