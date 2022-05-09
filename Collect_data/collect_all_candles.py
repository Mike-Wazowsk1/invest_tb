import time

import pandas as pd
import os
from datetime import timedelta
from datetime import datetime
from tqdm.auto import tqdm
import keyring
from tinkoff.invest import CandleInterval, Client, Quotation
from tinkoff.invest.utils import now
from tinkoff.invest import AsyncClient
import pyarrow
from tinkoff.invest.utils import quotation_to_decimal
from typing import List

TOKEN = keyring.get_password('TOKEN', 'INVEST')
SANDBOX_TOKEN = keyring.get_password('TOKEN', 'SANDBOX')
sandbox_account_id = keyring.get_password('ACCOUNT_ID', 'SANDBOX')


def get_shares_info():
    with Client(token=SANDBOX_TOKEN) as client:
        shares = client.instruments.shares().instruments
        figi = []
        ticker = []
        class_code = []
        isin = []
        lot = []
        currency = []
        klong = []
        kshort = []
        dlong = []
        dshort = []
        dlong_min = []
        dshort_min = []
        short_enabled_flag = []
        name = []
        exchange = []
        ipo_date = []
        issue_size = []
        country_of_risk = []
        country_of_risk_name = []
        sector = []
        issue_size_plan = []
        nominal = []
        trading_status = []
        otc_flag = []
        buy_available_flag = []
        sell_available_flag = []
        div_yield_flag = []
        share_type = []
        min_price_increment = []
        api_trade_available_flag = []
        uid = []
        real_exchange = []
        for share in shares:
            figi.append(share.figi)
            ticker.append(share.ticker)
            class_code.append(share.class_code)
            isin.append(share.isin)
            lot.append(share.lot)
            currency.append(share.currency)
            klong.append(quotation_to_decimal(share.klong))
            kshort.append(quotation_to_decimal(share.kshort))
            dlong.append(quotation_to_decimal(share.dlong))
            dshort.append(quotation_to_decimal(share.dshort))
            dlong_min.append(quotation_to_decimal(share.dlong_min))
            dshort_min.append(quotation_to_decimal(share.dshort_min))
            short_enabled_flag.append(quotation_to_decimal(share.dshort_min))
            name.append(share.name)
            exchange.append(share.exchange)
            ipo_date.append(share.ipo_date)
            issue_size.append(share.issue_size)
            country_of_risk.append(share.country_of_risk)
            country_of_risk_name.append(share.country_of_risk_name)
            sector.append(share.sector)
            issue_size_plan.append(share.issue_size_plan)
            nominal.append(quotation_to_decimal(Quotation(share.nominal.units, share.nominal.nano)))
            trading_status.append(share.trading_status)
            otc_flag.append(share.otc_flag)
            buy_available_flag.append(share.buy_available_flag)
            sell_available_flag.append(share.sell_available_flag)
            div_yield_flag.append(share.div_yield_flag)
            share_type.append(share.share_type)
            min_price_increment.append(quotation_to_decimal(share.min_price_increment))
            api_trade_available_flag.append(share.api_trade_available_flag)
            uid.append(share.uid)
            real_exchange.append(share.real_exchange)
        d = {'figi': figi, 'ticker': ticker, 'class_code': class_code, 'isin': isin, 'lot': lot,
             'currency': currency,
             'klong': klong, 'kshort': kshort, 'dlong': dlong, 'dshort': dshort, 'dlong_min': dlong_min,
             'dshort_min': dshort_min, 'short_enabled_flag': short_enabled_flag, 'name': name,
             'exchange': exchange, 'ipo_date': ipo_date, 'issue_size': issue_size, 'nominal': nominal,
             'trading_status': trading_status, 'otc_flag': otc_flag, 'buy_available_flag': buy_available_flag,
             'sell_available_flag': sell_available_flag, 'div_yield_flag': div_yield_flag,
             'share_type': share_type, 'min_price_increment': min_price_increment,
             'api_trade_available_flag': api_trade_available_flag, 'uid': uid, 'real_exchange': real_exchange
             }
        shares = pd.DataFrame(d)
        return shares


def create_dataset(list_candels: List = None, figi: str = None) -> pd.DataFrame:
    times = []
    opens = []
    closes = []
    highs = []
    lows = []
    volumes = []
    is_completes = []
    for candle in list_candels:
        highs.append(quotation_to_decimal(candle.high))
        lows.append(quotation_to_decimal(candle.low))
        opens.append(quotation_to_decimal(candle.open))
        closes.append(quotation_to_decimal(candle.close))
        volumes.append(candle.volume)
        times.append(candle.time)
        is_completes.append(candle.is_complete)
    df = pd.DataFrame(
        {'figi': figi, 'time': times, 'open': opens, 'close': closes, 'high': highs, 'low': lows, 'volume': volumes,
         'is_complete': is_completes})
    return df


def get_all_candles(idx):
    with Client(SANDBOX_TOKEN) as client:
        start = datetime(2000, 1, 1)
        tmp_df = []
        candles = []
        errors = {}
        f = []
        l = []
        for y in range(now().year - start.year + 1):
            if y == 22:
                try:
                    candle = client.market_data.get_candles(figi=idx,
                                                            from_=datetime(2022, 1, 1),
                                                            to=now(),
                                                            interval=CandleInterval(5)).candles
                    candles.append(candle)
                except:
                    f.append(idx)
                    l.append(y)
            else:
                try:
                    candle = client.market_data.get_candles(figi=idx,
                                                            from_=datetime(start.year + y, start.month, start.day),
                                                            to=datetime(start.year + y + 1, start.month, start.day),
                                                            interval=CandleInterval(5)).candles
                    candles.append(candle)
                except:
                    f.append(idx)
                    l.append(y)

        for c in candles:
            tmp_df.append(create_dataset(c, idx))
        errors['figi'] = f
        errors['y'] = l
    return tmp_df, errors


def create_shares_parquet(shares, fn):
    figi = shares.figi.values
    good_figi = []
    while len(good_figi) != len(figi):
        for idx in tqdm(figi):
            df, errors = fn(idx)
            good_figi.append(idx)
            time.sleep(1)
            df = pd.concat(df, ignore_index=True)
            df.to_parquet(f'Shares/2022_now/{idx}.parquet')
            errors = pd.DataFrame(errors)
            errors.to_parquet(f'Shares/2022_now/{idx}_err.parquet')
