delimiter $$
$$
CREATE TRIGGER gdax_fill_trig AFTER INSERT ON gdax_fill_hist
for each row
begin
	if new.symbol_pair = 'BTC-USD' THEN
		IF NEW.side = 'sell' THEN
			UPDATE gdax_bot_bal SET btc = btc - NEW.base_amt
				where bot_id = new.bot_id;
			UPDATE gdax_bot_bal SET usd = usd + NEW.quote_amt_inc_fee
				where bot_id = new.bot_id;
		ELSEIF NEW.side = 'buy' then
			UPDATE gdax_bot_bal SET btc = btc + NEW.base_amt
				where bot_id = new.bot_id;
			update gdax_bot_bal set usd = usd - new.quote_amt_inc_fee
				where bot_id = new.bot_id;
		end if;
	
	elseif new.symbol_pair = 'ETH-USD' THEN
		IF NEW.side = 'sell' THEN
			UPDATE gdax_bot_bal SET eth = eth - NEW.base_amt
				where bot_id = new.bot_id;
			UPDATE gdax_bot_bal SET usd = usd + NEW.quote_amt_inc_fee
				where bot_id = new.bot_id;
		ELSEIF NEW.side = 'buy' then
			UPDATE gdax_bot_bal SET eth = eth + NEW.base_amt
				where bot_id = new.bot_id;
			update gdax_bot_bal set usd = usd - new.quote_amt_inc_fee
				where bot_id = new.bot_id;
		end if;
	
	elseif new.symbol_pair = 'BCH-USD' THEN
		IF NEW.side = 'sell' THEN
			UPDATE gdax_bot_bal SET bch = bch - NEW.base_amt
				where bot_id = new.bot_id;
			UPDATE gdax_bot_bal SET usd = usd + NEW.quote_amt_inc_fee
				where bot_id = new.bot_id;
		ELSEIF NEW.side = 'buy' then
			UPDATE gdax_bot_bal SET bch = bch + NEW.base_amt
				where bot_id = new.bot_id;
			update gdax_bot_bal set usd = usd - new.quote_amt_inc_fee
				where bot_id = new.bot_id;
		end if;		
			
	elseif new.symbol_pair = 'BCH-BTC' THEN
		IF NEW.side = 'sell' THEN
			UPDATE gdax_bot_bal SET bch = bch - NEW.base_amt
				where bot_id = new.bot_id;
			UPDATE gdax_bot_bal SET btc = btc + NEW.quote_amt_inc_fee
				where bot_id = new.bot_id;
		ELSEIF NEW.side = 'buy' then
			UPDATE gdax_bot_bal SET bch = bch + NEW.base_amt
				where bot_id = new.bot_id;
			update gdax_bot_bal set btc = btc - new.quote_amt_inc_fee
				where bot_id = new.bot_id;
		end if;		

	elseif new.symbol_pair = 'LTC-BTC' THEN
		IF NEW.side = 'sell' THEN
			UPDATE gdax_bot_bal SET ltc = ltc - NEW.base_amt
				where bot_id = new.bot_id;
			UPDATE gdax_bot_bal SET btc = btc + NEW.quote_amt_inc_fee
				where bot_id = new.bot_id;
		ELSEIF NEW.side = 'buy' then
			UPDATE gdax_bot_bal SET ltc = ltc + NEW.base_amt
				where bot_id = new.bot_id;
			update gdax_bot_bal set btc = btc - new.quote_amt_inc_fee
				where bot_id = new.bot_id;
		end if;		

	elseif new.symbol_pair = 'ETH-BTC' THEN
		IF NEW.side = 'sell' THEN
			UPDATE gdax_bot_bal SET eth = eth - NEW.base_amt
				where bot_id = new.bot_id;
			UPDATE gdax_bot_bal SET btc = btc + NEW.quote_amt_inc_fee
				where bot_id = new.bot_id;
		ELSEIF NEW.side = 'buy' then
			UPDATE gdax_bot_bal SET eth = eth + NEW.base_amt
				where bot_id = new.bot_id;
			update gdax_bot_bal set btc = btc - new.quote_amt_inc_fee
				where bot_id = new.bot_id;
		end if;		
	end if;
END;
$$

$$
CREATE TRIGGER gdax_transfer_trig AFTER INSERT ON gdax_transfer_hist
for each row
begin
	if new.cur = 'BTC' THEN
		UPDATE gdax_bot_bal SET btc = btc + NEW.transfer_amt
				where bot_id = new.bot_id;
	elseif new.cur = 'USD' THEN
		UPDATE gdax_bot_bal SET usd = usd + NEW.transfer_amt
				where bot_id = new.bot_id;
	elseif new.cur = 'ETH' THEN
		UPDATE gdax_bot_bal SET eth = eth + NEW.transfer_amt
				where bot_id = new.bot_id;
	elseif new.cur = 'BCH' THEN
		UPDATE gdax_bot_bal SET bch = bch + NEW.transfer_amt
				where bot_id = new.bot_id;
	elseif new.cur = 'EUR' THEN
		UPDATE gdax_bot_bal SET eur = eur + NEW.transfer_amt
				where bot_id = new.bot_id;
	elseif new.cur = 'LTC' THEN
		UPDATE gdax_bot_bal SET ltc = ltc + NEW.transfer_amt
				where bot_id = new.bot_id;
	end if;
END;
$$

$$
CREATE TRIGGER gdax_order_aff AFTER INSERT ON gdax_order_cur
for each row
INSERT INTO gdax_order_aff
        (order_id, bot_id) values (new.order_id, new.bot_id);
$$


