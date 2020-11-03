import typing

from .listener import EventListener
from .emitter import EventEmitter
from .constants import (
    BALANCES_UPDATED_EVENT_KEY,
)


class BalanceEventListener(EventListener):
    def register_listeners(self):
        raise NotImplementedError()

    def register_balances_updated_listener(self, listener: typing.Callable):
        self.register_listener(BALANCES_UPDATED_EVENT_KEY, listener)


class BalanceEventEmitter(EventEmitter):
    async def emit_balances_updated_event(self, *args, **kwargs):
        await self.emit(BALANCES_UPDATED_EVENT_KEY, *args, **kwargs)
