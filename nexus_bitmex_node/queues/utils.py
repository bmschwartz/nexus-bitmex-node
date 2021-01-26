import aiormq
import typing
from aio_pika import Queue, Exchange


async def cleanup_queue(queue: Queue, exchange: Exchange, routing_key: typing.Optional[str] = None):
    try:
        await queue.purge()
    except aiormq.exceptions.ChannelInvalidStateError:
        pass

    await queue.unbind(exchange, routing_key=routing_key)

    try:
        await queue.delete(if_empty=False, if_unused=False)
    except aiormq.exceptions.ChannelInvalidStateError:
        pass
