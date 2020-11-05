from glom import glom

from nexus_bitmex_node.event_bus import (
    EventBus,
    event_bus,
    ExchangeEventEmitter,
)


class BitmexManager(ExchangeEventEmitter):
    _symbol_data: dict

    def __init__(self, bus: EventBus):
        ExchangeEventEmitter.__init__(self, bus)
        self._symbol_data = {}

    def update_ticker_data(self, data: dict):
        ticker_data_spec = {"symbol": "symbol", "price": "close_price"}

        info = glom(data, ticker_data_spec)
        self._symbol_data[info.get("symbol").lower()] = info

    def get_price(self, symbol: str):
        info = self._symbol_data.get(symbol.lower(), {})
        return info.get("price")


bitmex_manager = BitmexManager(event_bus)
