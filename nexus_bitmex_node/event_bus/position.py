import typing

from .listener import EventListener
from .emitter import EventEmitter
from .constants import (
    POSITION_UPDATED_EVENT_KEY,
)


class PositionEventListener(EventListener):
    def register_listeners(self):
        raise NotImplementedError()

    def register_position_updated_listener(self, listener: typing.Callable):
        self.register_listener(POSITION_UPDATED_EVENT_KEY, listener)


class PositionEventEmitter(EventEmitter):
    async def emit_position_updated_event(self, *args, **kwargs):
        await self.emit(POSITION_UPDATED_EVENT_KEY, *args, **kwargs)
