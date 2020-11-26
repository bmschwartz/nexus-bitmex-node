import json
import typing
from collections import defaultdict

import aioredis
from aioredis import Redis

from nexus_bitmex_node.models.order import XBt_TO_XBT_FACTOR
from nexus_bitmex_node.models.trade import BitmexTrade, create_trade
from nexus_bitmex_node.storage.data_store import DataStore


class RedisDataStore(DataStore):
    _client: Redis

    def register_listeners(self):
        self.register_margins_updated_listener(self.save_margins)
        self.register_ticker_updated_listener(self.save_tickers)
        self.register_my_trades_updated_listener(self.save_trades)
        self.register_positions_updated_listener(self.save_positions)

    async def start(self, url: str):
        self._client = await aioredis.create_redis_pool(url, encoding="utf-8")

    async def stop(self):
        self._client.close()
        await self._client.wait_closed()

    async def save_order(self, client_key: str):
        pass

    async def save_margins(self, client_key: str, data: typing.Dict):
        margins: typing.Dict = defaultdict(dict)

        for entry in data.get("info", []):
            currency = entry["currency"]
            balance = entry["marginBalance"]
            used = entry["maintMargin"]
            available = balance - used

            balance, used, available = (round(val * XBt_TO_XBT_FACTOR, 8) for val in (balance, used, available,))

            margins[currency] = json.dumps({
                "balance": balance,
                "used": used,
                "available": available,
            })

        self._client.hmset_dict(f"bitmex:{client_key}:margins", margins)

    async def save_tickers(self, client_key: str, data: typing.Dict):
        tickers: typing.Dict = {}
        for symbol, val in data.items():
            tickers[symbol] = json.dumps(val)
        self._client.hmset_dict(f"bitmex:{client_key}:tickers", tickers)

    async def save_trades(self, client_key: str, data: typing.List):
        stored: typing.Dict = {}
        trades: typing.List[BitmexTrade] = [create_trade(entry.get("info")) for entry in data]

        for trade in trades:
            trade_id = trade.order_id
            stored[trade_id] = trade.to_json()
        self._client.hmset_dict(f"bitmex:{client_key}:trades", stored)

    async def save_positions(self, client_key: str, data: typing.List):
        positions: typing.Dict = {}
        for entry in data:
            symbol = entry["symbol"]
            positions[symbol] = json.dumps(entry)
        self._client.hmset_dict(f"bitmex:{client_key}:positions", positions)

    async def get_order(self, client_key: str, order_id: str):
        pass

    async def get_margins(self, client_key: str):
        stored = typing.Dict = await self._client.hgetall(f"bitmex:{client_key}:margins", encoding="utf-8")
        margins: typing.Dict = {}
        for symbol, data in stored.items():
            margins[symbol] = json.loads(data)
        return margins

    async def get_margin(self, client_key: str, symbol: str):
        margins = await self.get_margins(client_key)
        return margins.get(symbol, {})

    async def get_positions(self, client_key: str):
        stored: typing.Dict = await self._client.hgetall(f"bitmex:{client_key}:positions", encoding="utf-8")
        positions: typing.Dict = {}
        for symbol, data in stored.items():
            positions[symbol] = json.loads(data)
        return positions

    async def get_position(self, client_key: str, symbol: str):
        positions = await self.get_positions(client_key)
        return positions.get(symbol, None)

    async def get_tickers(self, client_key: str):
        stored: typing.Dict = await self._client.hgetall(f"bitmex:{client_key}:tickers", encoding="utf-8")
        tickers: typing.Dict = {}
        for symbol, data in stored.items():
            tickers[symbol] = json.loads(data)
        return tickers

    async def get_ticker(self, client_key: str, symbol: str):
        stored: typing.List = await self._client.hmget(f"bitmex:{client_key}:tickers", symbol, encoding="utf-8")
        ticker: typing.Dict = {}
        for entry in stored:
            data = json.loads(entry)
            if data["symbol"] == symbol:
                ticker = data
                break
        return ticker
