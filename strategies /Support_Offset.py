import datetime
import time
from decimal import Decimal

import keyring
from tinkoff.invest import Client, OrderDirection, OrderType, Quotation
from tinkoff.invest.utils import decimal_to_quotation, quotation_to_decimal

from Basic_strategy import Strategy
import numpy as np

from User import User

USER = 'nikolay'
SQL_PASS = keyring.get_password('SQL', USER)
url = "jdbc:postgresql://localhost:5432/shares"
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


class Support_Offset(Strategy):
    def __init__(self, token, user, sql_pass, url):

        super().__init__(token=token, url=url, user=user, sql_pass=sql_pass)
        self.lots = self.sqler.read_sql("""
                    SELECT figi,lot from shares_info""").toPandas()

    def start(self, flag, account_id=None):
        while flag:
            with Client(token=self.token) as client:
                def find_quantity_to_buy(figi, curr_price):
                    lot = self.lots[self.lots.figi == f"{figi}"].lot.values[0]
                    money_to_actions = [m[1] for m in user.available_money if m[0] == account_id][0] * Decimal(0.1)
                    quantity = int(money_to_actions / (lot * quotation_to_decimal(curr_price)))
                    return quantity, lot

                user = User(client)
                start_time = time.time()
                order_id = str(np.random.random(1))
                current_prices = self.get_current_prices()
                prev_max = self.get_agg_all_figi_with_date(agg='max', col='close',
                                                           from_=datetime.datetime.now() - datetime.timedelta(days=3),
                                                           to='now', table='candles_day2022')
                prev_min = self.get_agg_all_figi_with_date(agg='max', col='close',
                                                           from_=datetime.datetime.now() - datetime.timedelta(days=3),
                                                           to='now', table='candles_day2022')

                def find_signals(curr, max_price, min_price):
                    sell = []
                    buy = []
                    for c in curr.keys():
                        if curr.get(c) >= max_price.get(c, np.inf):
                            sell.append({c: curr[c]})
                        elif curr.get(c) <= min_price.get(c, -np.inf):
                            buy.append({c: curr[c]})
                    return buy, sell

                buy, sell = find_signals(current_prices, prev_max, prev_min)

                if len(buy) != 0:
                    for d in buy:
                        figi = list(d.keys())[0]
                        price = decimal_to_quotation(list(d.values())[0])
                        q, lot = find_quantity_to_buy(figi, price)
                        if q > lot:
                            client.orders.post_order(figi=figi, quantity=q,
                                                     price=price,
                                                     direction=OrderDirection(1), account_id=account_id,
                                                     order_type=OrderType(2),
                                                     order_id=order_id)  # direct 1-buy 2 -sell| type 1-limit 2-market
                if len(sell) != 0:
                    for d in sell:
                        figi = list(d.keys())[0]
                        price = decimal_to_quotation(list(d.values())[0])
                        q, lot = find_quantity_to_buy(figi, price)
                        if q > lot:
                            client.orders.post_order(figi=figi, quantity=q,
                                                     price=price,
                                                     direction=OrderDirection(2), account_id=account_id,
                                                     order_type=OrderType(2),
                                                     order_id=order_id)  # direct 1-buy 2 -sell| type 1-limit 2-market

                def find_good_pos(pos):
                    sell = []
                    for p in pos:
                        hist = quotation_to_decimal(Quotation(
                            p.average_position_price.units,
                            p.average_position_price.nano)) * quotation_to_decimal(p.quantity)

                        curr = quotation_to_decimal(Quotation(
                            p.current_price.units,
                            p.current_price.nano)) * quotation_to_decimal(p.quantity)

                        if (curr + curr * user.fee) - hist >= hist * Decimal(0.01):
                            sell.append([p.figi, p.quantity])
                    return sell

                pos = [p[1][0] for p in user.possitions if p[0] == account_id]
                portfolio_sell = find_good_pos(pos)
                if len(portfolio_sell) != 0:
                    for d in portfolio_sell:
                        client.orders.post_order(figi=d[0], quantity=int(quotation_to_decimal(d[1])),
                                                 direction=OrderDirection(2), account_id=account_id,
                                                 order_type=OrderType(2),
                                                 order_id=order_id)  # direct 1-buy 2 -sell| type 1-limit 2-market
                print(time.time() - start_time)


