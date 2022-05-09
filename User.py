import keyring
from tinkoff.invest import Client, Quotation
from tinkoff.invest.services import Services
from decimal import Decimal

from tinkoff.invest.utils import quotation_to_decimal

TOKEN = keyring.get_password('TOKEN', 'INVEST')
SANDBOX_TOKEN = keyring.get_password('TOKEN', 'SANDBOX')
sandbox_account_id = keyring.get_password('ACCOUNT_ID', 'SANDBOX')


class User:
    def __init__(self, services: Services):

        self._services = services

        self.accounts = client.users.get_accounts().accounts
        self.limits = client.users.get_user_tariff()
        self.info = client.users.get_info()
        self.__init_info__()
        self.flag_russian_shares = True if 'russian_shares' in self.info.qualified_for_work_with else False
        self.flag_foreign_shares = True if 'foreign_shares' in self.info.qualified_for_work_with else False

        self.tariff = self.info.tariff
        if self.tariff == 'investor':
            self.fee = 0.3
            self.pay = 0
        elif self.tariff == 'trader':
            self.fee = 0.04
            self.pay = 0 if self.portfolio > 2000000 else 290
        elif self.tariff == 'premium':
            self.fee = 0.025
            self.pay = 1990

    def __init_info__(self):
        margin_info = []
        available_money = []
        positions = []
        shares = []
        expected_yield = []
        for account in self.accounts:
            account_id = account.id
            portfolio_response = self._services.operations.get_portfolio(
                account_id=account_id
            )
            amount_curr = portfolio_response.total_amount_currencies
            available_money.append(quotation_to_decimal(Quotation(units=amount_curr.units, nano=amount_curr.nano)))

            positions.append(portfolio_response.positions)
            amount_shares = portfolio_response.total_amount_shares
            shares.append(quotation_to_decimal(Quotation(units=amount_shares.units,nano=amount_shares.nano)))
            expected_yield.append(quotation_to_decimal(portfolio_response.expected_yield))
            try:
                margin_info.append(client.users.get_margin_attributes(account_id=account_id))
            except:
                margin_info.append('Disabled')

        self.available_money = available_money
        self.margin_info = margin_info
        self.possitions = positions
        self.shares = shares
        self.expected_yield = expected_yield
        self.portfolio = sum(self.shares) + sum(self.available_money)



with Client(token=TOKEN) as client:
    user = User(services=client)
    print(user.available_money)
    print(user.possitions)
    print(user.shares)
    print(user.portfolio)
    print(user.expected_yield)
