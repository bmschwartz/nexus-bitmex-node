import ccxtpro
from glom import glom

from nexus_bitmex_node.event_bus import (
    EventBus,
    event_bus,
    ExchangeEventEmitter,
)
from nexus_bitmex_node.event_bus.balance import BalanceEventEmitter


class BitmexManager(BalanceEventEmitter, ExchangeEventEmitter):
    _symbol_data: dict
    _client: ccxtpro.bitmex

    def __init__(self, bus: EventBus):
        BalanceEventEmitter.__init__(self, bus)
        ExchangeEventEmitter.__init__(self, bus)

        self._symbol_data = {}

    async def start(self, client: ccxtpro.bitmex):
        self._client = client

        while True:
            balances = await self._client.watch_balance()
            trades = await self._client.watch_my_trades()

            print(balances)
            print(trades)

    def update_ticker_data(self, data: dict):
        ticker_data_spec = {"symbol": "symbol", "price": "close_price"}

        info = glom(data, ticker_data_spec)
        self._symbol_data[info.get("symbol").lower()] = info

    def get_price(self, symbol: str):
        info = self._symbol_data.get(symbol.lower(), {})
        return info.get("price")


bitmex_manager = BitmexManager(event_bus)
