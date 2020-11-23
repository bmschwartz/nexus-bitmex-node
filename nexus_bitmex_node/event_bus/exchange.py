import typing

from .listener import EventListener
from .emitter import EventEmitter
from .constants import (
    TICKER_UPDATED_EVENT_KEY,
    MARGINS_UPDATED_EVENT_KEY,
    POSITIONS_UPDATED_EVENT_KEY,
    MY_TRADES_UPDATED_EVENT_KEY,
)


class ExchangeEventListener(EventListener):
    def register_listeners(self):
        raise NotImplementedError()

    def register_ticker_updated_listener(self, listener: typing.Callable):
        self.register_listener(TICKER_UPDATED_EVENT_KEY, listener)

    def register_margins_updated_listener(self, listener: typing.Callable):
        self.register_listener(MARGINS_UPDATED_EVENT_KEY, listener)

    def register_positions_updated_listener(self, listener: typing.Callable):
        self.register_listener(POSITIONS_UPDATED_EVENT_KEY, listener)

    def register_my_trades_updated_listener(self, listener: typing.Callable):
        self.register_listener(MY_TRADES_UPDATED_EVENT_KEY, listener)


class ExchangeEventEmitter(EventEmitter):
    async def emit_ticker_updated_event(self, *args, **kwargs):
        await self.emit(TICKER_UPDATED_EVENT_KEY, *args, **kwargs)

    async def emit_margins_updated_event(self, *args, **kwargs):
        await self.emit(MARGINS_UPDATED_EVENT_KEY, *args, **kwargs)

    async def emit_positions_updated_event(self, *args, **kwargs):
        await self.emit(POSITIONS_UPDATED_EVENT_KEY, *args, **kwargs)

    async def emit_my_trades_updated_event(self, *args, **kwargs):
        await self.emit(MY_TRADES_UPDATED_EVENT_KEY, *args, **kwargs)
