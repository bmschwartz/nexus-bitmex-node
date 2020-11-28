import typing

from nexus_bitmex_node.event_bus import EventListener, EventEmitter
from .constants import (
    CREATE_ACCOUNT_CMD_KEY,
    UPDATE_ACCOUNT_CMD_KEY,
    DELETE_ACCOUNT_CMD_KEY,

    ACCOUNT_HEARTBEAT_KEY,

    ACCOUNT_CREATED_EVENT_KEY,
    ACCOUNT_UPDATED_EVENT_KEY,
    ACCOUNT_DELETED_EVENT_KEY,
)


class AccountEventListener(EventListener):
    def register_listeners(self):
        raise NotImplementedError()

    # Command listeners
    def register_create_account_listener(self, listener: typing.Callable, loop):
        self.register_listener(CREATE_ACCOUNT_CMD_KEY, listener, loop)

    def register_update_account_listener(self, listener: typing.Callable, loop):
        self.register_listener(UPDATE_ACCOUNT_CMD_KEY, listener, loop)

    def register_delete_account_listener(self, listener: typing.Callable, loop):
        self.register_listener(DELETE_ACCOUNT_CMD_KEY, listener, loop)

    def register_account_heartbeat_listener(self, listener: typing.Callable, loop):
        self.register_listener(ACCOUNT_HEARTBEAT_KEY, listener, loop)

    # Result listeners
    def register_account_created_listener(self, listener: typing.Callable, loop):
        self.register_listener(ACCOUNT_CREATED_EVENT_KEY, listener, loop)

    def register_account_updated_listener(self, listener: typing.Callable, loop):
        self.register_listener(ACCOUNT_UPDATED_EVENT_KEY, listener, loop)

    def register_account_deleted_listener(self, listener: typing.Callable, loop):
        self.register_listener(ACCOUNT_DELETED_EVENT_KEY, listener, loop)


class AccountEventEmitter(EventEmitter):
    # Command events
    async def emit_create_account_event(self, *args, **kwargs):
        await self.emit(CREATE_ACCOUNT_CMD_KEY, *args, **kwargs)

    async def emit_update_account_event(self, *args, **kwargs):
        await self.emit(UPDATE_ACCOUNT_CMD_KEY, *args, **kwargs)

    async def emit_delete_account_event(self, *args, **kwargs):
        await self.emit(DELETE_ACCOUNT_CMD_KEY, *args, **kwargs)

    # Result events
    async def emit_account_created_event(self, *args, **kwargs):
        await self.emit(ACCOUNT_CREATED_EVENT_KEY, *args, **kwargs)

    async def emit_account_updated_event(self, *args, **kwargs):
        await self.emit(ACCOUNT_UPDATED_EVENT_KEY, *args, **kwargs)

    async def emit_account_deleted_event(self, *args, **kwargs):
        await self.emit(ACCOUNT_DELETED_EVENT_KEY, *args, **kwargs)

    async def emit_account_heartbeat(self, *args, **kwargs):
        await self.emit(ACCOUNT_HEARTBEAT_KEY, *args, **kwargs)
