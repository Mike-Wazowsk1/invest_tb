import keyring
from tinkoff.invest import CandleInterval, Client

from tinkoff.invest import Client
import logging
from tinkoff.invest.services import Services
from stats.stats_data import Stats
from User import User

logger = logging.getLogger(__name__)

figi = "BBG000HLJ7M4"
TOKEN = keyring.get_password('TOKEN', 'INVEST')
SANDBOX_TOKEN = keyring.get_password('TOKEN', 'SANDBOX')
sandbox_account_id = keyring.get_password('ACCOUNT_ID', 'SANDBOX')


def select_strategy():
    pass

def main():
    with Client(token=TOKEN) as client:
        # select strategy for current user and figi
        strategy = select_strategy()


        stats = Stats(client)
        historical_data = stats.get_historical_candles(figi=figi, interval=CandleInterval(5), period='y')
        features = stats.make_features(df=historical_data,ma_window=5,ema_window=30)
        print(features)

if __name__ == "__main__":
    main()
