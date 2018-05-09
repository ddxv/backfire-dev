import mysql_prices as ms


engine = ms.connect_mysql()



gdax_df = ms.get_gdax_prices('2017-01-01')




