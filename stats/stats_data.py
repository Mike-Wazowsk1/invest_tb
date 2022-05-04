from datetime import datetime
from typing import Dict, List
from sklearn.linear_model import LinearRegression
import pandas as pd
from tinkoff.invest import InstrumentIdType, CandleInterval

from tinkoff.invest.utils import now


class Stats:
    def __init__(self, client):
        """

        :param client: Client main object to collect stats data

        """
        self._client = client

    def get_book(self, depth, instrument_id=None, ticker=None, uid=None, figi=None) -> Dict:
        """
        Create book with bid/ask for instrument at current time
        :param depth: count observations from book
        :param instrument_id: type of instrument: 0: unspecified 1: figi, 2: ticker, 3: uid
        :param ticker: unique name for instrument
        :param uid: unique id for instrument
        :param figi: unique id for instrument
        :return: dict[ask,bid] with float values
        """

        book = {"bid": [], "ask": []}

        instru = self._client.instruments.get_instrument_by(id_type=InstrumentIdType(1), id=figi)
        lot = instru.instrument.lot
        bids = self._client.market_data.get_order_book(figi=figi, depth=depth).bids
        ask = self._client.market_data.get_order_book(figi=figi, depth=depth).asks

        for i in range(len(bids)):
            book["bid"].append(float(str(bids[i].price.units) + '.' + str(bids[i].price.nano)) * lot)
        for i in range(len(ask)):
            book["ask"].append(float(str(ask[i].price.units) + '.' + str(ask[i].price.nano)) * lot)

        return book

    def get_historical_candles(self, figi: str, interval: int, period: str = 'y') -> pd.DataFrame:

        """
        Make pandas DataFrame for historical data with current period
        :param figi: Unique id for instrument
        :param interval: interval between observation in data:
            0: unspecified
            1: 1 min
            2: 5 min
            3: 15 min
            4: hour
            5: day
        :param period: period of data now can: 'y' for year 'm' for month
        :return: pandas DataFrame[time,open,close,high,low,volume,is_complete]
        """

        if period == 'y':
            candles = self._client.market_data.get_candles(
                figi=figi,
                from_=datetime(now().year - 1, now().month, now().day),
                to=now(),
                interval=CandleInterval(interval)).candles
        if period == 'mon':
            candles = self._client.market_data.get_candles(
                figi=figi,
                from_=datetime(now().year, now().month - 1, now().day),
                to=now(),
                interval=CandleInterval(interval)).candles

        def create_dataset(list_candels: List = None) -> pd.DataFrame:
            times = []
            opens = []
            closes = []
            highs = []
            lows = []
            volumes = []
            is_completes = []
            for candle in list_candels:
                highs.append(float(str(candle.high.units) + '.' + str(candle.high.nano)))
                lows.append(float(str(candle.low.units) + '.' + str(candle.low.nano)))
                opens.append(float(str(candle.open.units) + '.' + str(candle.open.nano)))
                closes.append(float(str(candle.close.units) + '.' + str(candle.close.nano)))
                volumes.append(candle.volume)
                times.append(candle.time)
                is_completes.append(candle.is_complete)
            df = pd.DataFrame(
                {'time': times, 'open': opens, 'close': closes, 'high': highs, 'low': lows, 'volume': volumes,
                 'is_complete': is_completes})
            return df

        return create_dataset(candles)

    @staticmethod
    def make_features(df, flag_ma: bool = True, flag_trend: bool = True, flag_ema=True,
                      ma_window=None, ema_window=None):
        if type(ma_window) == int:
            ma_window = [ma_window]
        if type(ema_window) == int:
            ema_window = [ema_window]
        df['mean'] = (df['high'] + df['low']) / 2
        if flag_ma:
            for window in ma_window:
                df[f'ma{window}'] = df['mean'].rolling(window).mean()
                df[f'ma{window}'].where(df[f'ma{window}'].notna(), df['mean'], inplace=True)
        if flag_ema:
            for window in ema_window:
                df[f'ema{window}'] = df['mean'].ewm(span=20, min_periods=0, adjust=False, ignore_na=False).mean()
                df[f'ema{window}'].where(df[f'ema{window}'].notna(), df['mean'], inplace=True)

        if flag_trend:
            reg = LinearRegression()
            X = df.drop(['close', 'time'], axis=1)
            y = df['close']
            reg.fit(X, y)
            df['trend'] = reg.predict(X)

        return df

    def get_last_prices(self):
        # Doesn't work?
        pass
