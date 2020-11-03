import abc
import json

import aioredis
import typing
from aioredis import Redis

from nexus_bitmex_node.event_bus import (
    EventListener,
    event_bus,
    EventBus,
    ExchangeEventListener,
)
from nexus_bitmex_node.event_bus.balance import BalanceEventListener


class DataStore(abc.ABC, EventListener):
    @abc.abstractmethod
    async def start(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    async def stop(self):
        ...

    @abc.abstractmethod
    async def save_order(self, client_key: str):
        ...

    @abc.abstractmethod
    async def save_balances(self, client_key: str, data: typing.Dict):
        ...

    @abc.abstractmethod
    async def save_position(self, client_key: str):
        ...

    @abc.abstractmethod
    async def get_order(self, client_key: str, order_id: str):
        ...

    @abc.abstractmethod
    async def get_balances(self, client_key: str):
        ...

    @abc.abstractmethod
    async def get_balance(self, client_key: str, symbol: str):
        ...

    @abc.abstractmethod
    async def get_position(self, client_key: str):
        ...


class RedisDataStore(DataStore, BalanceEventListener, ExchangeEventListener):
    _client: Redis

    def register_listeners(self):
        self.register_balances_updated_listener(self.save_balances)
        self.register_ticker_updated_listener(self.save_tickers)

    async def start(self, url: str):
        self._client = await aioredis.create_redis_pool(url, encoding="utf-8")

    async def stop(self):
        self._client.close()
        await self._client.wait_closed()

    async def save_order(self, client_key: str):
        pass

    async def save_balances(self, client_key: str, data: typing.Dict):
        for k, v in data.items():
            data[k] = json.dumps(v)

        await self._client.hmset_dict(f"bitmex:{client_key}:balances", data)

    async def save_tickers(self, data: typing.Dict):
        for k, v in data.items():
            data[k] = json.dumps(v)

        await self._client.hmset_dict(f"bitmex:tickers", data)

    async def save_position(self, client_key: str):
        pass

    async def get_order(self, client_key: str, order_id: str):
        pass

    async def get_balances(self, client_key: str):
        return self._client.get(f"bitmex:{client_key}:balances", encoding="utf-8")

    async def get_balance(self, client_key: str, symbol: str):
        pass

    async def get_position(self, client_key: str):
        pass

    def get_tickers(self):
        return self._client.get("balance:tickers", encoding="utf-8")

    def get_ticker(self, symbol: str):
        return self._client.hmget("balance.tickers", symbol.lower(), encoding="utf-8")


def create_data_store(bus: EventBus) -> DataStore:
    return RedisDataStore(bus)


data_store = create_data_store(event_bus)
