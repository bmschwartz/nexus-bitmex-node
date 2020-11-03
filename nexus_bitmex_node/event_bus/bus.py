import typing
from collections import defaultdict


class EventBus:
    _events: typing.Dict

    def __init__(self):
        self._events = defaultdict(set)

    async def publish(self, event_key, *args, **kwargs):
        """
        Calls the callback methods listening to the `event_key` event
        :param event_key:
        :param args:
        :param kwargs:
        :return:
        """
        for cb in self._events[event_key]:
            await cb(*args, **kwargs)

    def register(self, event_key, callback):
        """
        Registers another callback function to `event_key` event
        :param event_key:
        :param callback:
        :return:
        """
        # TODO: Add ability to unregister
        #  -> return a "key" to caller that they can use to .unregister(key)
        self._events[event_key].add(callback)
