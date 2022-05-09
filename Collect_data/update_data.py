from tinkoff.invest import Client, CandleInterval
from tinkoff.invest.utils import now

from Collect_data.collect_all_candles import create_dataset
from sql_supporter.sql_sup import Sqler
import keyring
from datetime import datetime
from tqdm.auto import tqdm
import pandas as pd
from multiprocessing.pool import ThreadPool
import time

TOKEN = keyring.get_password('TOKEN', 'INVEST')
SANDBOX_TOKEN = keyring.get_password('TOKEN', 'SANDBOX')
sandbox_account_id = keyring.get_password('ACCOUNT_ID', 'SANDBOX')


def update_candles(idx, from_):
    with Client(SANDBOX_TOKEN) as client:
        candle = client.market_data.get_candles(figi=idx,
                                                from_=from_,
                                                to=now(),
                                                interval=CandleInterval(5)).candles

        tmp_df = create_dataset(candle, idx)
    return tmp_df


def create_shares_parquet(shares, fn):
    figi = shares.figi.values
    for idx in tqdm(figi):
        df, errors = fn(idx)
        df = pd.concat(df, ignore_index=True)
        df.to_parquet(f'Shares/2022_now/{idx}.parquet')
        errors = pd.DataFrame(errors)
        errors.to_parquet(f'Shares/2022_now/{idx}_err.parquet')


USER = 'nikolay'
SQL_PASS = keyring.get_password('SQL', USER)
url = "jdbc:postgresql://localhost:5432/shares"
sqler = Sqler(url=url, user=USER, password=SQL_PASS)
figi = sqler.read_sql("""
        SELECT distinct figi from candles_day""").collect()
n = datetime.now()
good_figi = []
while len(good_figi) != len(figi):
    for idx in tqdm(figi):
        last_day = sqler.get_last_date_by_figi(idx[0])
        delta = n - last_day
        if delta.days >= 1 and idx[0] not in good_figi:
            try:
                df = update_candles(idx=idx[0], from_=last_day)
                sqler.insert(df,'candles_day')
                good_figi.append(idx[0])
            except:
                continue
        else:
            continue

