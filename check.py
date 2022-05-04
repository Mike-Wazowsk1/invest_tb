import keyring
from tinkoff.invest import CandleInterval, Client
from tinkoff.invest.utils import now
from datetime import datetime
from tinkoff.invest import Client, RequestError, OrderDirection, OrderType, Quotation, InstrumentIdType
import logging
import pandas as pd
from typing import Dict, List

logger = logging.getLogger(__name__)

figi = "BBG004730N88"
TOKEN = keyring.get_password('TOKEN', 'INVEST')
SANDBOX_TOKEN = keyring.get_password('TOKEN', 'SANDBOX')
sandbox_account_id = keyring.get_password('ACCOUNT_ID', 'SANDBOX')


def select_strategy():
    pass


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


def main():
    with Client(token=TOKEN) as client:
        # select strategy for current user and figi
        strategy = select_strategy()

        stats = Stats(client)
        print(stats.get_book(20, figi=figi))
        historical_data = stats.get_historical_candles(figi=figi, interval=CandleInterval(5), period='y')
        pr = client.market_data.get_last_prices(figi=figi)
        print(pr)
        print(historical_data)


if __name__ == "__main__":
    main()
