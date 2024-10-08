import typing

from . import EventListener, EventEmitter
from .constants import (
    CREATE_ORDER_CMD_KEY,
    UPDATE_ORDER_CMD_KEY,
    CANCEL_ORDER_CMD_KEY,

    ORDER_CREATED_EVENT_KEY,
    ORDER_UPDATED_EVENT_KEY,
    ORDER_CANCELED_EVENT_KEY,
)


class OrderEventListener(EventListener):
    def register_listeners(self):
        raise NotImplementedError()

    def register_create_order_listener(self, listener: typing.Callable, loop, rate_limit: float = None):
        self.register_listener(CREATE_ORDER_CMD_KEY, listener, loop, rate_limit)

    def register_update_order_listener(self, listener: typing.Callable, loop, rate_limit: float = None):
        self.register_listener(UPDATE_ORDER_CMD_KEY, listener, loop, rate_limit)

    def register_cancel_order_listener(self, listener: typing.Callable, loop, rate_limit: float = None):
        self.register_listener(CANCEL_ORDER_CMD_KEY, listener, loop, rate_limit)

    def register_order_created_listener(self, listener: typing.Callable, loop, rate_limit: float = None):
        self.register_listener(ORDER_CREATED_EVENT_KEY, listener, loop, rate_limit)

    def register_order_updated_listener(self, listener: typing.Callable, loop, rate_limit: float = None):
        self.register_listener(ORDER_UPDATED_EVENT_KEY, listener, loop, rate_limit)

    def register_order_canceled_listener(self, listener: typing.Callable, loop, rate_limit: float = None):
        self.register_listener(ORDER_CANCELED_EVENT_KEY, listener, loop, rate_limit)


class OrderEventEmitter(EventEmitter):
    async def emit_create_order_event(self, *args, **kwargs):
        await self.emit(CREATE_ORDER_CMD_KEY, *args, **kwargs)

    async def emit_update_order_event(self, *args, **kwargs):
        await self.emit(UPDATE_ORDER_CMD_KEY, *args, **kwargs)

    async def emit_cancel_order_event(self, *args, **kwargs):
        await self.emit(CANCEL_ORDER_CMD_KEY, *args, **kwargs)

    async def emit_order_created_event(self, *args, **kwargs):
        await self.emit(ORDER_CREATED_EVENT_KEY, *args, **kwargs)

    async def emit_order_updated_event(self, *args, **kwargs):
        await self.emit(ORDER_UPDATED_EVENT_KEY, *args, **kwargs)

    async def emit_order_canceled_event(self, *args, **kwargs):
        await self.emit(ORDER_CANCELED_EVENT_KEY, *args, **kwargs)
