from nexus_bitmex_node.event_bus.bus import EventBus


class EventListener:
    _registered: bool

    def __init__(self, event_bus: EventBus):
        self._event_bus: EventBus = event_bus

        if not getattr(self, "_registered", False):
            self.register_listeners()
            self._registered = True

    def register_listeners(self):
        raise NotImplementedError()

    def register_listener(self, event_key, callback, loop, rate_limit: float = None):
        self._event_bus.register(event_key, callback, loop, rate_limit)
