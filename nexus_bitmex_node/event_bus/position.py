import typing

from . import EventListener, EventEmitter
from .constants import (
    POSITION_CLOSE_CMD_KEY,
    POSITION_ADD_STOP_CMD_KEY,
    POSITION_ADD_TSL_CMD_KEY,

    POSITION_CLOSED_EVENT_KEY,
    POSITION_ADDED_STOP_EVENT_KEY,
    POSITION_ADDED_TSL_EVENT_KEY,
)


class PositionEventListener(EventListener):
    def register_listeners(self):
        raise NotImplementedError()

    def register_close_position_listener(self, listener: typing.Callable):
        self.register_listener(POSITION_CLOSE_CMD_KEY, listener)

    def register_add_stop_to_position_listener(self, listener: typing.Callable):
        self.register_listener(POSITION_ADD_STOP_CMD_KEY, listener)

    def register_add_tsl_to_position_listener(self, listener: typing.Callable):
        self.register_listener(POSITION_ADD_TSL_CMD_KEY, listener)

    def register_position_closed_listener(self, listener: typing.Callable):
        self.register_listener(POSITION_CLOSED_EVENT_KEY, listener)

    def register_added_stop_to_position_event(self, listener: typing.Callable):
        self.register_listener(POSITION_ADDED_STOP_EVENT_KEY, listener)

    def register_added_tsl_to_position_event(self, listener: typing.Callable):
        self.register_listener(POSITION_ADDED_TSL_EVENT_KEY, listener)


class PositionEventEmitter(EventEmitter):
    async def emit_close_position_event(self, *args, **kwargs):
        await self.emit(POSITION_CLOSE_CMD_KEY, *args, **kwargs)

    async def emit_position_add_stop_event(self, *args, **kwargs):
        await self.emit(POSITION_ADD_STOP_CMD_KEY, *args, **kwargs)

    async def emit_position_add_tsl_event(self, *args, **kwargs):
        await self.emit(POSITION_ADD_TSL_CMD_KEY, *args, **kwargs)

    async def emit_position_closed_event(self, *args, **kwargs):
        await self.emit(POSITION_CLOSED_EVENT_KEY, *args, **kwargs)

    async def emit_added_stop_to_position_event(self, *args, **kwargs):
        await self.emit(POSITION_ADDED_STOP_EVENT_KEY, *args, **kwargs)

    async def emit_added_tsl_to_position_event(self, *args, **kwargs):
        await self.emit(POSITION_ADDED_TSL_EVENT_KEY, *args, **kwargs)
