import abc
import typing

from nexus_bitmex_node.event_bus import ExchangeEventListener


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
    async def save_order(self, client_key: str):
        ...

    @abc.abstractmethod
    async def save_margins(self, client_key: str, data: typing.Dict):
        ...

    @abc.abstractmethod
    async def save_tickers(self, client_key: str, data: typing.Dict):
        ...

    @abc.abstractmethod
    async def save_my_trades(self, client_key: str, data: typing.List):
        ...

    @abc.abstractmethod
    async def save_positions(self, client_key: str, data: typing.List):
        ...

    @abc.abstractmethod
    async def get_order(self, client_key: str, order_id: str):
        ...

    @abc.abstractmethod
    async def get_margins(self, client_key: str):
        ...

    @abc.abstractmethod
    async def get_margin(self, client_key: str, symbol: str):
        ...

    @abc.abstractmethod
    async def get_positions(self, client_key: str):
        ...

    @abc.abstractmethod
    async def get_position(self, client_key: str, symbol: str):
        ...

    @abc.abstractmethod
    async def get_tickers(self, client_key: str):
        ...

    @abc.abstractmethod
    async def get_ticker(self, client_key: str, symbol: str):
        ...

