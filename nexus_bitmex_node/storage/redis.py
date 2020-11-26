import json
import typing
from collections import defaultdict

import aioredis
from aioredis import Redis

from nexus_bitmex_node.models.order import XBt_TO_XBT_FACTOR, BitmexOrder, create_order
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

    """ Orders """
    async def save_order(self, client_key: str, order: BitmexOrder):
        self._client.hmset_dict(f"bitmex:{client_key}:orders", order.id, order.to_json())

    async def get_orders(self, client_key: str) -> typing.Dict[str, BitmexOrder]:
        stored = typing.Dict = await self._client.hgetall(f"bitmex:{client_key}:orders", encoding="utf-8")
        orders: typing.Dict[str, BitmexOrder] = {}
        for order_id, data in stored.items():
            orders[order_id] = create_order(json.loads(data))
        return orders

    async def get_order(self, client_key: str, order_id: str) -> typing.Optional[BitmexOrder]:
        element = await self._get_single_match_key_element("orders", client_key, order_id, "order_id")
        return create_order(element) if element else None

    """ Margins """
    async def save_margins(self, client_key: str, data: typing.Dict):
        margins: typing.Dict = defaultdict(dict)
        to_store = await self.get_margins(client_key)
        for entry in data.get("info", []):
            currency = entry["currency"]
            balance = entry["marginBalance"]
            used = entry["maintMargin"]
            available = balance - used

            balance, used, available = (round(val * XBt_TO_XBT_FACTOR, 8) for val in (balance, used, available,))

            margin_data = json.dumps({
                "balance": balance,
                "used": used,
                "available": available,
            })
            to_store.update({currency: margin_data})

        self._client.hmset_dict(f"bitmex:{client_key}:margins", to_store)

    async def get_margins(self, client_key: str):
        stored = typing.Dict = await self._client.hgetall(f"bitmex:{client_key}:margins", encoding="utf-8")
        margins: typing.Dict = {}
        for symbol, data in stored.items():
            margins[symbol] = json.loads(data)
        return margins

    async def get_margin(self, client_key: str, symbol: str):
        return await self._get_single_match_key_element("margins", client_key, symbol, "symbol") or None

    """ Positions """
    async def save_positions(self, client_key: str, data: typing.List):
        to_store = await self.get_positions(client_key)
        for entry in data:
            symbol = entry["symbol"]
            to_store.update({symbol: json.dumps(entry)})
        self._client.hmset_dict(f"bitmex:{client_key}:positions", to_store)

    async def get_positions(self, client_key: str):
        stored: typing.Dict = await self._client.hgetall(f"bitmex:{client_key}:positions", encoding="utf-8")
        positions: typing.Dict = {}
        for symbol, data in stored.items():
            positions[symbol] = json.loads(data)
        return positions

    async def get_position(self, client_key: str, symbol: str):
        element = await self._get_single_match_key_element("positions", client_key, symbol, "symbol")
        return create_trade(element) if element else None

    """ Trades """
    async def save_trades(self, client_key: str, data: typing.List):
        new_trades: typing.List[BitmexTrade] = [create_trade(entry.get("info")) for entry in data]
        to_store = await self.get_trades(client_key)
        for trade in new_trades:
            trade_id = trade.order_id
            to_store.update({trade_id: trade.to_json()})
        self._client.hmset_dict(f"bitmex:{client_key}:trades", to_store)

    async def get_trades(self, client_key: str):
        stored = typing.Dict = await self._client.hgetall(f"bitmex:{client_key}:trades", encoding="utf-8")
        trades: typing.Dict[str, BitmexTrade] = {}
        for trade_id, data in stored.items():
            trades[trade_id] = create_trade(json.loads(data))
        return trades

    async def get_trade(self, client_key: str, order_id: str) -> typing.Optional[BitmexTrade]:
        element = await self._get_single_match_key_element("trades", client_key, order_id, "order_id")
        return create_trade(element) if element else None

    """ Tickers """
    async def save_tickers(self, client_key: str, data: typing.Dict):
        to_store = await self.get_trades(client_key)
        for symbol, val in data.items():
            to_store.update({symbol: json.dumps(val)})
        self._client.hmset_dict(f"bitmex:{client_key}:tickers", to_store)

    async def get_tickers(self, client_key: str):
        stored: typing.Dict = await self._client.hgetall(f"bitmex:{client_key}:tickers", encoding="utf-8")
        tickers: typing.Dict = {}
        for symbol, data in stored.items():
            tickers[symbol] = json.loads(data)
        return tickers

    async def get_ticker(self, client_key: str, symbol: str):
        return await self._get_single_match_key_element("tickers", client_key, symbol, "symbol") or None

    async def _get_single_match_key_element(self, type_key: str, client_key: str, entry_key: str, match_key: str):
        stored = await self._client.hmget(f"bitmex:{client_key}:{type_key}", entry_key, encoding="utf-8")
        for entry in stored:
            data = json.loads(entry)
            if data[match_key] == entry_key:
                return data
        else:
            return None
