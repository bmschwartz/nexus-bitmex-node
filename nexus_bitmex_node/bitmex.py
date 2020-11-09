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

    def __init__(self, bus: EventBus):
        BalanceEventEmitter.__init__(self, bus)
        ExchangeEventEmitter.__init__(self, bus)

        self._symbol_data = {}

    async def watch_streams(self, client: ccxtpro.bitmex):
        while True:

            self.update_position_data(client.positions)
            await client.sleep(2000)

    def update_position_data(self, data: list):
        self.emit_balances_updated_event()


bitmex_manager = BitmexManager(event_bus)
