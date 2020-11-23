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


class DataStore(abc.ABC, EventListener):
    @abc.abstractmethod
    async def start(self, *args, **kwargs):
        ...

    @abc.abstractmethod
    async def stop(self):
        ...

    @abc.abstractmethod
    def register_listeners(self):
        ...


class RedisDataStore(DataStore, ExchangeEventListener):
    _client: Redis

    def register_listeners(self):
        self.register_margins_updated_listener(self.save_margins)
        self.register_ticker_updated_listener(self.save_tickers)
        self.register_my_trades_updated_listener(self.save_my_trades)
        self.register_positions_updated_listener(self.save_positions)

    async def start(self, url: str):
        self._client = await aioredis.create_redis_pool(url, encoding="utf-8")

    async def stop(self):
        self._client.close()
        await self._client.wait_closed()

    async def save_order(self, client_key: str):
        pass

    async def save_margins(self, client_key: str, data: typing.List):
        margins: typing.Dict = {}
        for entry in data:
            symbol = entry["currency"]
            margins[symbol] = json.dumps(entry)
        self._client.hmset_dict(f"bitmex:{client_key}:margins", margins)

    async def save_tickers(self, data: typing.Dict):
        for k, v in data.items():
            data[k] = json.dumps(v)

        self._client.hmset_dict(f"bitmex:tickers", data)

    async def save_my_trades(self, client_key: str, data: typing.List):
        trades: typing.Dict = {}
        for entry in data:
            trade_id = entry["orderID"]
            trades[trade_id] = json.dumps(entry)
        print("saving trades")
        self._client.hmset_dict(f"bitmex:{client_key}:trades", trades)

    async def save_positions(self, client_key: str, data: typing.List):
        print(f"positions {data}")

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
