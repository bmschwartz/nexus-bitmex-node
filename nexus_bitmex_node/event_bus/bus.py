import asyncio
import time
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
        for cb, loop, rate_limit, last_call in self._events[event_key]:
            now = time.time() * 1000
            do_call = True if not rate_limit else (now - last_call >= rate_limit)
            last_call = now
            if do_call:
                asyncio.ensure_future(cb(*args, **kwargs), loop=loop)
            else:
                print(f"not calling {event_key}")

    def register(self, event_key, callback, loop, rate_limit: float = None):
        """
        Registers another callback function to `event_key` event to be run on `loop`
        :param event_key:
        :param callback:
        :param loop:
        :param rate_limit: [Optional] If provided, ignores consecutive messages sent in this time window (milliseconds)
        :return:
        """
        now = time.time() * 1000
        self._events[event_key].add((callback, loop, rate_limit, now))
