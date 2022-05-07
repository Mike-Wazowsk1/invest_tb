from tinkoff.invest import Client, Quotation
from tinkoff.invest.services import Services
from decimal import Decimal

from tinkoff.invest.utils import quotation_to_decimal


class User:
    def __init__(self, token,services: Services):
        self._services = services
        self._token = token
        self.margin_info = []
        with Client(self._token) as client:
            self.accounts = client.users.get_accounts().accounts
            self.tariff = client.users.get_user_tariff()
            self.info = client.users.get_info()
            for acc in self.accounts:
                try:
                    self.margin_info.append(client.users.get_margin_attributes(account_id=acc.id))
                except:
                    self.margin_info.append('Disabled')

    def get_current_balance(self) -> Decimal:
        for account in self.accounts:
            account_id = account.id
            portfolio_response = self._services.operations.get_portfolio(
                account_id=account_id
            )
            balance = portfolio_response.total_amount_currencies
        return quotation_to_decimal(Quotation(units=balance.units, nano=balance.nano))
