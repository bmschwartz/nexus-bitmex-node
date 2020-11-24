import json
import typing

import aioredis
from aioredis import Redis

from nexus_bitmex_node.storage.data_store import DataStore


class RedisDataStore(DataStore):
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

    async def save_tickers(self, client_key: str, data: typing.Dict):
        tickers: typing.Dict = {}
        for symbol, val in data.items():
            tickers[symbol] = json.dumps(val)
        self._client.hmset_dict(f"bitmex:{client_key}:tickers", tickers)

    async def save_my_trades(self, client_key: str, data: typing.List):
        trades: typing.Dict = {}
        for entry in data:
            trade_id = entry["orderID"]
            trades[trade_id] = json.dumps(entry)
        self._client.hmset_dict(f"bitmex:{client_key}:trades", trades)

    async def save_positions(self, client_key: str, data: typing.List):
        positions: typing.Dict = {}
        for entry in data:
            symbol = entry["symbol"]
            positions[symbol] = json.dumps(entry)
        self._client.hmset_dict(f"bitmex:{client_key}:positions", positions)

    async def get_order(self, client_key: str, order_id: str):
        pass

    async def get_balances(self, client_key: str):
        return self._client.hmget(f"bitmex:{client_key}:balances", encoding="utf-8")

    async def get_balance(self, client_key: str, symbol: str):
        pass

    async def get_positions(self, client_key: str):
        stored: typing.Dict = await self._client.hgetall(f"bitmex:{client_key}:positions", encoding="utf-8")
        positions: typing.Dict = {}
        for symbol, data in stored.items():
            positions[symbol] = json.loads(data)
        return positions

    async def get_position(self, client_key: str, symbol: str):
        positions = await self.get_positions(client_key)
        return positions[symbol]

    def get_tickers(self):
        return self._client.hmget("balance:tickers", encoding="utf-8")

    def get_ticker(self, symbol: str):
        return self._client.hmget("balance.tickers", symbol.lower(), encoding="utf-8")
