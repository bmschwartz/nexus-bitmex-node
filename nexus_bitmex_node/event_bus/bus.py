import asyncio
import typing
from collections import defaultdict


class EventBus:
    _events: typing.Dict

    def __init__(self):
        self._events = defaultdict(set)

    async def publish(self, event_key, *args, **kwargs):
        """
        Asynchronously calls the callback methods listening to the `event_key` event
        :param event_key:
        :param args:
        :param kwargs:
        :return:
        """
        for cb, loop in self._events[event_key]:
            asyncio.ensure_future(cb(*args, **kwargs), loop=loop)

    def publish_sync(self, event_key, *args, **kwargs):
        """
        Synchronously calls the callback methods listening to the `event_key` event
        :param event_key:
        :param args:
        :param kwargs:
        :return:
        """
        for cb in self._events[event_key]:
            cb(*args, **kwargs)

    def register(self, event_key, callback, loop):
        """
        Registers another callback function to `event_key` event to be run on `loop`
        :param event_key:
        :param callback:
        :param loop:
        :return:
        """
        self._events[event_key].add((callback, loop))
