import abc
import typing

from nexus_bitmex_node.event_bus import ExchangeEventListener
from nexus_bitmex_node.models.order import BitmexOrder
from nexus_bitmex_node.models.position import BitmexPosition
from nexus_bitmex_node.models.trade import BitmexTrade


class DataStore(abc.ABC, ExchangeEventListener):
    @abc.abstractmethod
    async def start(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    async def stop(self):
        ...

    @abc.abstractmethod
    def register_listeners(self):
        ...

    @abc.abstractmethod
    async def save_order(self, client_key: str, order: BitmexOrder):
        ...

    @abc.abstractmethod
    async def save_margins(self, client_key: str, data: typing.Dict):
        ...

    @abc.abstractmethod
    async def save_tickers(self, client_key: str, data: typing.Dict):
        ...

    @abc.abstractmethod
    async def save_trades(self, client_key: str, data: typing.List[BitmexTrade]):
        ...

    @abc.abstractmethod
    async def save_positions(self, client_key: str, data: typing.List):
        ...

    @abc.abstractmethod
    async def get_orders(self, client_key: str) -> typing.Dict[str, BitmexOrder]:
        ...

    @abc.abstractmethod
    async def get_order(self, client_key: str, order_id: str) -> typing.Optional[BitmexOrder]:
        ...

    @abc.abstractmethod
    async def get_margins(self, client_key: str):
        ...

    @abc.abstractmethod
    async def get_margin(self, client_key: str, symbol: str):
        ...

    @abc.abstractmethod
    async def get_positions(self, client_key: str) -> typing.Dict[str, BitmexPosition]:
        ...

    @abc.abstractmethod
    async def get_position(self, client_key: str, symbol: str) -> typing.Optional[BitmexPosition]:
        ...

    @abc.abstractmethod
    async def get_trades(self, client_key: str):
        ...

    @abc.abstractmethod
    async def get_trade(self, client_key: str, trade_id: str) -> typing.Optional[BitmexTrade]:
        ...

    @abc.abstractmethod
    async def get_tickers(self, client_key: str):
        ...

    @abc.abstractmethod
    async def get_ticker(self, client_key: str, symbol: str):
        ...

