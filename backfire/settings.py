

# EMAFeatures should only be the EMA specific features ?
# TODO: Make backtest settings class for min / max / prinipal?

class EMASettings:
    bot_id = 'default'
    def set_bot_id(self, var):
        self.bot_id = var
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
    buy_pct_usd = 0
    def set_buy_pct_usd(self, val):
        self.buy_pct_usd = val
    sell_pct_btc = 0
    def set_sell_pct_btc(self, val):
        self.sell_pct_btc = val
    principle_btc = 0
    def set_principle_btc(self, val):
        self.principle_btc = val
    principle_usd = 0
    def set_principle_usd(self, val):
        self.principle_usd = val
    min_usd = 0
    def set_min_usd(self, val):
        self.min_usd = val
    min_btc = 0
    def set_min_btc(self, val):
        self.min_btc = val
    gdax_fee_pct = 0
    def set_gdax_fee_pct(self, val):
        self.gdax_fee_pct = val



