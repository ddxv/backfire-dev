use gdax_test;

create database gdax;

drop table if exists 
gdax_order_cur,
gdax_order_hist,
gdax_fill_hist,
gdax_bot_bal,
gdax_transfer_hist, 
gdax_order_aff;

create table
gdax_order_cur (
order_id VARCHAR(64) not null,
signal_id VARCHAR(64) not null,
bot_id VARCHAR(64) not null,
price DECIMAL(20,8),
base_amt DECIMAL(20,8) not null,
symbol_pair	VARCHAR(10) not null,
side VARCHAR(10) not null,
order_type VARCHAR(12) not null,
time_in_force VARCHAR(8),
post_only TINYINT(1),
created_at timestamp(6),
fill_fees DECIMAL(20,16),
filled_amt DECIMAL(18,8),
executed_value DECIMAL(20,16),
status VARCHAR(12),
settled TINYINT(1),
primary key (order_id)
);


create table
gdax_order_aff (
order_id VARCHAR(64) not null,
bot_id VARCHAR(64) not null,
primary key (order_id)
);


create table
gdax_order_hist (
order_id VARCHAR(64) not null,
price DECIMAL(20,8),
base_amt DECIMAL(20,8) not null,
symbol_pair	VARCHAR(10) not null,
side VARCHAR(10) not null,
order_type VARCHAR(12) not null,
time_in_force VARCHAR(8),
post_only TINYINT(1),
created_at timestamp(6),
fill_fees DECIMAL(20,16),
filled_amt DECIMAL(18,8),
executed_value DECIMAL(20,16),
status VARCHAR(12),
settled TINYINT(1),
primary key (order_id)
);





create table
gdax_fill_hist (
created_at timestamp(3) not null,
trade_id INT not null,
bot_id VARCHAR(64) not null,
symbol_pair	VARCHAR(10) not null,
base_symbol VARCHAR(4) not null,
quote_symbol VARCHAR(4) not null,
order_id VARCHAR(64) not null,
user_id varchar(64) not null,	
profile_id VARCHAR(64) not null,
liquidity VARCHAR(4) not null,
price DECIMAL(20,8) not null,
base_amt DECIMAL(20,8) not null,
quote_amt DECIMAL(20,16) not null,
quote_amt_inc_fee DECIMAL(20,16) not null,
fee DECIMAL(20,16) not null,
side VARCHAR(10) not null,
settled TINYINT(1) not null,
amt_usd DECIMAL(22, 16),
primary key (trade_id),
foreign key (order_id) references gdax_order_aff(order_id)
);



create table
gdax_bot_bal (
bot_id VARCHAR(64) not null,
usd DECIMAL(24, 16) not null,
eur DECIMAL(24, 16) not null,
btc DECIMAL(24, 16) not null,
eth DECIMAL(24, 16) not null,
bch DECIMAL(24, 16) not null,
ltc DECIMAL(24, 16) not null,
primary key (bot_id)
);


create table
gdax_transfer_hist (
transfer_amt decimal(20,16) not null,
created_at timestamp(5) not null,
cur VARCHAR(4) not null,
trade_id VARCHAR(64) not null,
transfer_id VARCHAR(64) not null,
transfer_type VARCHAR(20) not null,
bot_id VARCHAR(20) not null,
primary key (transfer_id)
);

