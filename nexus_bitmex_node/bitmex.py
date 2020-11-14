import ccxtpro

from nexus_bitmex_node.event_bus import (
    EventBus,
    event_bus,
    ExchangeEventEmitter,
)
from nexus_bitmex_node.event_bus.balance import BalanceEventEmitter


class BitmexManager(BalanceEventEmitter, ExchangeEventEmitter):
    _symbol_data: dict
    _client: ccxtpro.bitmex
    _client_id: str
    _watching_streams: bool

    def __init__(self, bus: EventBus):
        BalanceEventEmitter.__init__(self, bus)
        ExchangeEventEmitter.__init__(self, bus)

        self._client_id = ''
        self._symbol_data = {}

    def stop_streams(self):
        self._watching_streams = False

    async def watch_streams(self, client_id: str, client: ccxtpro.bitmex):
        self._client_id = client_id
        self._watching_streams = True

        while self._watching_streams:
            await self.update_position_data(client.positions)
            await client.sleep(2000)

    async def update_position_data(self, data: list):
        await self.emit_balances_updated_event(self._client_id, data)


bitmex_manager = BitmexManager(event_bus)
