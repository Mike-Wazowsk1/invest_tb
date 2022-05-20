import datetime
import time
from decimal import Decimal

import keyring
from tinkoff.invest import Client, OrderDirection, OrderType, Quotation
from tinkoff.invest.utils import decimal_to_quotation, quotation_to_decimal


import numpy as np

from User import User
from tinkoff.invest import Client

from tinkoff.invest.utils import quotation_to_decimal,decimal_to_quotation

from Collect_data.sql_supporter.sql_sup import Sqler

import time


TOKEN = keyring.get_password('TOKEN', 'INVEST')
SANDBOX_TOKEN = keyring.get_password('TOKEN', 'SANDBOX')
sandbox_account_id = keyring.get_password('ACCOUNT_ID', 'SANDBOX')





def time_of_function(function):
    def wrapped(*args):
        start_time = time.time()
        res = function(*args)
        print(time.time() - start_time)
        return res

    return wrapped


class Strategy:
    def __init__(self, token, url, user, sql_pass):
        self.sqler = Sqler(url=url, user=user, password=sql_pass)
        self.sc = self.sqler.spark.sparkContext
        figi = self.sqler.read_sql("""SELECT distinct(figi) from candles_day_rus""").collect()
        self.figi = [row.figi for row in figi]
        self.token = token

    def get_agg_with_date_by_figi(self, agg, col,figi, from_, to, table):
        df = self.sqler.select_agg_with_date_figi(agg, col,figi, from_, to, table).collect()
        result = df[0][0]
        return result

    def get_agg_all_figi_with_date(self, agg, col, from_, to, table):
        df = self.sqler.select_agg_with_date_all_figi(agg, col, from_, to, table).collect()
        result = {row.figi: row[f'{agg}'] for row in df}
        return result

    def get_current_prices(self):
        with Client(self.token) as client:
            prices = client.market_data.get_last_prices(figi=self.figi).last_prices
            figi_arr = [row.figi for row in prices]
            price_arr = [row.price for row in prices]
            current_df = {figi: quotation_to_decimal(price) for figi, price in zip(figi_arr, price_arr)}
        return current_df


class MovingAverage(Strategy):
    pass