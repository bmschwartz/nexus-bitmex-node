import json
import sys
import typing
from typing import Dict, Any
from collections import defaultdict

from nexus_bitmex_node.models.order import XBt_TO_XBT_FACTOR, BitmexOrder, create_order
from nexus_bitmex_node.models.trade import BitmexTrade, create_trade
from nexus_bitmex_node.storage.data_store import DataStore


class LocalDataStoreClient:
    _cache: typing.Dict

    def __init__(self):
        self._cache = {}

    def close(self):
        self._cache = {}

    def put(self, key: str, data: Any):
        self._cache[key] = data

    def put_dict(self, key: str, field: str, data: Any):
        if key not in self._cache:
            self._cache[key] = {}
        self._cache[key][field] = data

    def get(self, key: str) -> Dict[Any, Any]:
        return self._cache.get(key, {})

    def get_field(self, key: str, field: str) -> Any:
        return self._cache.get(key, {}).get(field)


class LocalDataStore(DataStore):
    _client: LocalDataStoreClient

    def register_listeners(self):
        loop = asyncio.get_event_loop()
        self.register_margins_updated_listener(self.save_margins, loop)
        self.register_ticker_updated_listener(self.save_tickers, loop)
        self.register_trades_updated_listener(self.save_trades, loop)
        self.register_positions_updated_listener(self.save_positions, loop)
        self.register_order_placed_listener(self.save_order, loop)

    async def start(self):
        self._client = LocalDataStoreClient()

    async def stop(self):
        self._client.close()

    """ Orders """
    async def save_order(self, client_key: str, order: BitmexOrder):
        self._client.put_dict(f"bitmex:{client_key}:orders", str(order.id), order.to_json())

    async def get_orders(self, client_key: str) -> typing.Dict[str, BitmexOrder]:
        stored: Dict = self._client.get(f"bitmex:{client_key}:orders")
        orders: Dict[str, BitmexOrder] = {}
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

            margin_data = {
                "balance": balance,
                "used": used,
                "available": available,
            }
            to_store.update({currency: margin_data})

        self._client.put(f"bitmex:{client_key}:margins", to_store)

    async def get_margins(self, client_key: str):
        stored: Dict = self._client.get(f"bitmex:{client_key}:margins")
        margins: typing.Dict = {}
        for symbol, data in stored.items():
            margins[symbol] = data
        return margins

    async def get_margin(self, client_key: str, symbol: str):
        return self._client.get_field(f"bitmex:{client_key}:margins", symbol)

    """ Positions """
    async def save_positions(self, client_key: str, data: typing.List):
        to_store = await self.get_positions(client_key)
        for entry in data:
            symbol = entry["symbol"]
            to_store.update({symbol: json.dumps(entry)})
        self._client.put(f"bitmex:{client_key}:positions", to_store)

    async def get_positions(self, client_key: str):
        stored: typing.Dict = self._client.get(f"bitmex:{client_key}:positions")
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
        to_store = await self.get_trades(client_key, as_json=True)
        for trade in new_trades:
            trade_id = trade.order_id
            to_store.update({trade_id: trade.to_json()})
        self._client.put(f"bitmex:{client_key}:trades", to_store)

    async def get_trades(self, client_key: str, as_json=False):
        stored: typing.Dict = self._client.get(f"bitmex:{client_key}:trades")
        if as_json:
            return stored

        trades: typing.Dict[str, BitmexTrade] = {}
        for trade_id, data in stored.items():
            trades[trade_id] = create_trade(json.loads(data))
        return trades

    async def get_trade(self, client_key: str, order_id: str) -> typing.Optional[BitmexTrade]:
        element = await self._get_single_match_key_element("trades", client_key, order_id, "order_id")
        return create_trade(element) if element else None

    """ Tickers """
    async def save_tickers(self, client_key: str, data: typing.Dict):
        to_store = await self.get_tickers(client_key)
        for symbol, val in data.items():
            to_store.update({symbol: json.dumps(val)})
        self._client.put(f"bitmex:{client_key}:tickers", to_store)

    async def get_tickers(self, client_key: str):
        stored: typing.Dict = self._client.get(f"bitmex:{client_key}:tickers")
        tickers: typing.Dict = {}
        for symbol, data in stored.items():
            tickers[symbol] = json.loads(data)
        return tickers

    async def get_ticker(self, client_key: str, symbol: str):
        return await self._get_single_match_key_element("tickers", client_key, symbol, "symbol") or None

    async def _get_single_match_key_element(self, type_key: str, client_key: str, entry_key: str, match_key: str):
        stored = self._client.get(f"bitmex:{client_key}:{type_key}")
        for entry in stored.values():
            data = json.loads(entry)
            if data[match_key] == entry_key:
                return data
        else:
            return None
