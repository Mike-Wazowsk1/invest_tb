import time

import keyring
from tinkoff.invest import CandleInterval, Client

from tinkoff.invest import Client
import logging
from tinkoff.invest.services import Services
from joblib import Parallel, delayed

from Collect_data.sql_supporter.sql_sup import Sqler
from stats.stats_data import Stats
from User import User
from datetime import datetime, timedelta
from tqdm.auto import tqdm
import pickle
from multiprocessing.pool import ThreadPool

def get_all_market():
    pass


USER = 'nikolay'
SQL_PASS = keyring.get_password('SQL', USER)
url = "jdbc:postgresql://localhost:5432/shares"


class Support_Offset:
    def __init__(self):
        self.sqler = Sqler(url=url, user=USER, password=SQL_PASS)
        self.sc = self.sqler.spark.sparkContext
        self.figi = self.sqler.read_sql("""SELECT distinct(figi) from candles_day""").collect()

    def get_max_week_price(self, figi):
        price = self.sqler.select_agg_with_date(agg='max', col='close', figi=figi,
                                                from_=datetime.now() - timedelta(days=7),
                                                to='now', table='candles_day')

        return price

    def get_all_max_week_price(self):


        return self.sqler.read_sql("""
            SELECT figi,MAX(open) from candles_day2022
            group by figi""").collect()


simple_str = Support_Offset()
start = time.time()
res = simple_str.get_all_max_week_price()
