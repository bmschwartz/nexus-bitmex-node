from nexus_bitmex_node.event_bus.bus import EventBus


class EventEmitter:
    def __init__(self, event_bus: EventBus):
        self._event_bus: EventBus = event_bus

    async def emit(self, event_key, *args, **kwargs):
        await self._event_bus.publish(event_key, *args, **kwargs)

    def emit_sync(self, event_key, *args, **kwargs):
        self._event_bus.publish_sync(event_key, *args, **kwargs)
