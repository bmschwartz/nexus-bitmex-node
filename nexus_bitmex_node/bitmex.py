import typing
import asyncio

import ccxtpro

from nexus_bitmex_node.event_bus import (
    EventBus,
    event_bus,
    ExchangeEventEmitter,
)


class BitmexManager(ExchangeEventEmitter):
    _symbol_data: dict
    _client: ccxtpro.bitmex
    _client_id: str
    _watching_streams: bool

    def __init__(self, bus: EventBus):
        ExchangeEventEmitter.__init__(self, bus)

        self._client_id = ''
        self._symbol_data = {}

    def stop_streams(self):
        self._watching_streams = False

    async def watch_streams(self, client_id: str, client: ccxtpro.bitmex):
        self._client_id = client_id
        self._watching_streams = True

        while self._watching_streams:
            await self.update_margin_data(client.balance)
            await self.update_my_trades_data(client.myTrades)
            await self.update_positions_data(client.positions)
            await asyncio.sleep(2)

    async def update_margin_data(self, data: typing.Dict):
        margins = data.get("info", [])
        await self.emit_margins_updated_event(self._client_id, margins)

    async def update_positions_data(self, data: typing.Dict):
        await self.emit_positions_updated_event(self._client_id, list(data.values()))

    async def update_my_trades_data(self, data: typing.Dict):
        if not data:
            return

        trades = [trade.get("info") for trade in data]
        await self.emit_my_trades_updated_event(self._client_id, trades)


bitmex_manager = BitmexManager(event_bus)
