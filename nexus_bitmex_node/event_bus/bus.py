import asyncio
import time
import typing
from collections import defaultdict


class EventBus:
    _events: typing.Dict

    def __init__(self):
        self._events = defaultdict(dict)

    async def publish(self, event_key, *args, **kwargs):
        """
        Asynchronously calls the callback methods listening to the `event_key` event
        :param event_key:
        :param args:
        :param kwargs:
        :return:
        """
        for cb, info in self._events[event_key].items():

            now = time.time() * 1000
            last_call = info["last_call"]
            rate_limit = info["rate_limit"]

            do_call = True if not rate_limit else (now - last_call >= rate_limit)

            if do_call:
                asyncio.ensure_future(cb(*args, **kwargs), loop=info["loop"])
                self._events[event_key][cb].update({"last_call": now})

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
        self._events[event_key].update({callback: {"loop": loop, "rate_limit": rate_limit, "last_call": now}})
