import typing
from aio_pika import Connection, Channel

QUEUE_EXPIRATION_TIME = 1800000  # ms


class QueueManager:
    recv_connection: Connection
    send_connection: Connection
    channels: typing.List[Channel]

    def __init__(self, recv_connection: Connection, send_connection: Connection):
        self.recv_connection = recv_connection
        self.send_connection = send_connection
        self.channels = []

    async def start(self):
        await self.create_channels()
        await self.declare_exchanges()
        await self.declare_queues()

    async def stop(self):
        for channel in self.channels:
            if channel and not channel.is_closed:
                await channel.close()

    async def create_channels(self):
        raise NotImplementedError()

    def create_channel(self, connection: Connection, **channel_kwargs):
        channel = connection.channel(**channel_kwargs)
        self.channels.append(channel)

        return channel

    async def declare_exchanges(self):
        raise NotImplementedError()

    async def declare_queues(self):
        raise NotImplementedError()
