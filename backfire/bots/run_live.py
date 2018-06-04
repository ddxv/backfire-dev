from backfire.bots.bot_db import avail_balances
from backfire.bots.gdax_func import initialize_gdax, update_gdax_transfers_manual
from backfire.bots.trade import check_signal_loop
from backfire.settings import EMASettings
import logging
FORMAT = '%(asctime)s: %(name)s: %(levelname)s: %(message)s'
logging.basicConfig(format = FORMAT, filename = 'trade.log', level = logging.INFO)
formatter = logging.Formatter(FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not len(logger.handlers):
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    stderrLogger=logging.StreamHandler()
    stderrLogger.setFormatter(formatter)
    logging.getLogger().addHandler(stderrLogger)

bot_vars = EMASettings()
# Initialize Bot Settings
bot_vars.set_gdax_fee_pct(.003)
bot_vars.set_bot_id('bot_test')
bot_vars.set_principle_usd(40)
bot_vars.set_principle_btc(.04)
bot_vars.set_upper_window(2)
bot_vars.set_lower_window(2)
bot_vars.set_factor_high(0.00001)
bot_vars.set_factor_low(0.00001)
bot_vars.set_buy_pct_usd(0.05)
bot_vars.set_sell_pct_btc(0.052)
bot_vars.set_min_usd(78)
bot_vars.set_min_btc(.001)

#LIVE ACCOUNT
#ac = initialize_gdax(False)
#mult = 1

#SANDBOX
ac = initialize_gdax(True)

update_gdax_transfers_manual(ac)


base_symbol = 'BTC'
quote_symbol = 'USD'

avail_base, avail_quote = avail_balances(bot_vars.bot_id, base_symbol, quote_symbol)
logger.info(f'{bot_vars.bot_id}: Bot Starting: {base_symbol}: {avail_base}, {quote_symbol}: {avail_quote}')

# Loops forever
check_signal_loop(ac, bot_vars, base_symbol, quote_symbol)


