import json
import time
import typing
import asyncio
from json import JSONDecodeError
from uuid import uuid4

from aio_pika import (
    Queue,
    Connection,
    Channel,
    Exchange,
    ExchangeType,
    IncomingMessage,
    Message,
    DeliveryMode,
)

from nexus_bitmex_node.event_bus import PositionEventEmitter, EventBus, PositionEventListener, AccountEventListener, \
    ExchangeEventListener
from nexus_bitmex_node.models.position import create_position
from nexus_bitmex_node.queues.position.helpers import (
    handle_close_position_message,
    handle_add_stop_to_position_message,
    handle_add_tsl_to_position_message,
)
from nexus_bitmex_node.queues.queue_manager import QueueManager, QUEUE_EXPIRATION_TIME
from nexus_bitmex_node.settings import BITMEX_EXCHANGE

from nexus_bitmex_node.queues.position.constants import (
    BITMEX_POSITION_CLOSE_CMD_PREFIX,
    BITMEX_POSITION_ADD_STOP_CMD_PREFIX,
    BITMEX_POSITION_ADD_TSL_CMD_PREFIX,
    BITMEX_POSITION_CLOSE_QUEUE_PREFIX,
    BITMEX_POSITION_ADD_STOP_QUEUE_PREFIX,
    BITMEX_POSITION_ADD_TSL_QUEUE_PREFIX,
    BITMEX_POSITION_CLOSED_EVENT_KEY,
    BITMEX_POSITION_ADDED_STOP_EVENT_KEY,
    BITMEX_POSITION_TSL_ADDED_EVENT_KEY,
    BITMEX_POSITION_UPDATED_EVENT_KEY,
)


POSITION_UPDATE_INTERVAL = 10000  # ms


class PositionQueueManager(
    QueueManager,
    ExchangeEventListener,
    PositionEventEmitter,
    PositionEventListener,
    AccountEventListener
):
    _recv_position_channel: Channel
    _send_position_channel: Channel

    _recv_bitmex_exchange: Exchange
    _send_bitmex_exchange: Exchange

    _close_position_queue: Queue
    _position_add_stop_queue: Queue
    _position_add_tsl_queue: Queue

    _close_position_routing_key: str
    _position_add_stop_routing_key: str
    _position_add_tsl_routing_key: str

    _attached_consumers: bool

    _last_update: float

    def __init__(
        self,
        event_bus: EventBus,
        recv_connection: Connection,
        send_connection: Connection,
    ):
        QueueManager.__init__(self, recv_connection, send_connection)
        PositionEventEmitter.__init__(self, event_bus)
        AccountEventListener.__init__(self, event_bus)
        ExchangeEventListener.__init__(self, event_bus)

        self._last_update = time.time()
        self._close_position_consumer_tag = str(uuid4())
        self._position_add_stop_consumer_tag = str(uuid4())
        self._position_add_tsl_consumer_tag = str(uuid4())

    async def start(self):
        await super(PositionQueueManager, self).start()

    async def declare_queues(self):
        # Can't declare any queues on startup because we don't have a linked account
        pass

    async def create_channels(self):
        self._recv_position_channel = await self.create_channel(self.recv_connection)
        self._send_position_channel = await self.create_channel(self.send_connection)
        await self._recv_position_channel.set_qos(prefetch_count=1)

    def register_listeners(self):
        loop = asyncio.get_event_loop()
        self.register_account_created_listener(self.listen_to_position_queues, loop)
        self.register_account_deleted_listener(self.stop_listening_to_position_queues, loop)
        self.register_positions_updated_listener(self._on_positions_updated, loop, rate_limit=POSITION_UPDATE_INTERVAL)
        self.register_position_closed_listener(self._on_position_closed, loop)
        self.register_added_stop_to_position_event(self._on_position_added_stop, loop)
        self.register_added_tsl_to_position_event(self._on_position_added_tsl, loop)

    async def declare_exchanges(self):
        self._recv_bitmex_exchange = await self._recv_position_channel.declare_exchange(
            BITMEX_EXCHANGE, type=ExchangeType.TOPIC, durable=True
        )

        self._send_bitmex_exchange = await self._send_position_channel.declare_exchange(
            BITMEX_EXCHANGE, type=ExchangeType.TOPIC, durable=True
        )

    async def _on_positions_updated(self, account_id: str,  data: typing.List, error: Exception = None) -> None:
        positions = [create_position(position).to_json() for position in data]

        response_payload: dict = {
            "positions": positions,
            "accountId": account_id,
            "exchange": "BITMEX",
            "success": error is None,
            "error": error,
        }

        response = Message(
            bytes(json.dumps(response_payload), "utf-8"),
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type="application/json",
        )

        await self._send_bitmex_exchange.publish(
            response, routing_key=BITMEX_POSITION_UPDATED_EVENT_KEY
        )

    async def _on_position_closed(self, message_id: str, position: typing.Dict, error: Exception = None) -> None:
        response_payload: dict = {
            "position": position,
            "success": error is None,
            "error": error,
        }

        response = Message(
            bytes(json.dumps(response_payload), "utf-8"),
            delivery_mode=DeliveryMode.PERSISTENT,
            correlation_id=message_id,
            content_type="application/json",
        )
        await self._send_bitmex_exchange.publish(
            response, routing_key=BITMEX_POSITION_CLOSED_EVENT_KEY
        )

    async def _on_position_added_stop(self, position_id: str) -> None:
        # TODO: Do something now that an order has been updated
        pass

    async def _on_position_added_tsl(self, position_id: str) -> None:
        # TODO: Do something now that an order has been canceled
        pass

    async def listen_to_position_queues(self, account_id: str):
        await self.stop_listening_to_position_queues()
        await self._declare_position_queues(account_id)
        self._set_position_routing_keys(account_id)
        await self._bind_position_queues()

        if not getattr(self, "_attached_consumers", False):
            self._attached_consumers = True
            await self._attach_consumers()

    async def stop_listening_to_position_queues(self):
            self._attached_consumers = False

        if getattr(self, "_close_position_queue", None):
            await self._close_position_queue.unbind(self._recv_bitmex_exchange)
            await self._close_position_queue.delete()

        if getattr(self, "_position_add_stop_queue", None):
            await self._position_add_stop_queue.unbind(self._recv_bitmex_exchange)
            await self._position_add_stop_queue.delete()

        if getattr(self, "_position_add_tsl_queue", None):
            await self._position_add_tsl_queue.unbind(self._recv_bitmex_exchange)
            await self._position_add_tsl_queue.delete()

    def _set_position_routing_keys(self, account_id: str):
        self._close_position_routing_key = f"{BITMEX_POSITION_CLOSE_CMD_PREFIX}{account_id}"
        self._position_add_stop_routing_key = f"{BITMEX_POSITION_ADD_STOP_CMD_PREFIX}{account_id}"
        self._position_add_tsl_routing_key = f"{BITMEX_POSITION_ADD_TSL_CMD_PREFIX}{account_id}"

    async def _declare_position_queues(self, account_id: str):
        # Declare queues
        self._close_position_queue = await self._recv_position_channel.declare_queue(
            f"{BITMEX_POSITION_CLOSE_QUEUE_PREFIX}{account_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

        self._position_add_stop_queue = await self._recv_position_channel.declare_queue(
            f"{BITMEX_POSITION_ADD_STOP_QUEUE_PREFIX}{account_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

        self._position_add_tsl_queue = await self._recv_position_channel.declare_queue(
            f"{BITMEX_POSITION_ADD_TSL_QUEUE_PREFIX}{account_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

    async def _bind_position_queues(self):
        await self._close_position_queue.bind(
            self._send_bitmex_exchange, self._close_position_routing_key
        )
        await self._position_add_stop_queue.bind(
            self._send_bitmex_exchange, self._position_add_stop_routing_key
        )
        await self._position_add_tsl_queue.bind(
            self._send_bitmex_exchange, self._position_add_tsl_routing_key
        )

    async def _attach_consumers(self):
        await self._close_position_queue.consume(
            self.on_close_position_message, consumer_tag=self._close_position_consumer_tag
        )
        await self._position_add_stop_queue.consume(
            self.on_position_add_stop_message, consumer_tag=self._position_add_stop_consumer_tag
        )
        await self._position_add_tsl_queue.consume(
            self.on_position_add_tsl_message, consumer_tag=self._position_add_tsl_consumer_tag
        )

        self._attached_consumers = True

    async def on_close_position_message(self, message: IncomingMessage):
        async with message.process(ignore_processed=True):
            position_id = None
            response_payload: dict = {}

            try:
                position_data = await handle_close_position_message(message)
                if position_data:
                    message.ack()
                    await self.emit_close_position_event(message.correlation_id, position_data)
                    return
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            else:
                response_payload.update({"success": False, "error": "Unknown Error"})

            response = Message(
                bytes(json.dumps(response_payload), "utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
                correlation_id=message.correlation_id,
                content_type="application/json",
            )
            await self._send_bitmex_exchange.publish(
                response, routing_key=BITMEX_POSITION_CLOSED_EVENT_KEY
            )

            message.ack()

    async def on_position_add_stop_message(self, message: IncomingMessage):
        async with message.process():
            response_payload: dict = {}

            try:
                position_data = await handle_add_stop_to_position_message(message)
                if position_data:
                    message.ack()
                    await self.emit_position_add_stop_event(message.correlation_id, position_data)
                    return
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            else:
                response_payload.update({"success": False, "error": "Unknown Error"})

            response = Message(
                bytes(json.dumps(response_payload), "utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
                correlation_id=message.correlation_id,
                content_type="application/json",
            )
            await self._send_bitmex_exchange.publish(
                response, routing_key=BITMEX_POSITION_ADDED_STOP_EVENT_KEY
            )

            message.ack()

    async def on_position_add_tsl_message(self, message: IncomingMessage):
        async with message.process(ignore_processed=True):
            response_payload: dict = {}

            try:
                position_data = await handle_add_stop_to_position_message(message)
                if position_data:
                    message.ack()
                    await self.emit_position_add_tsl_event(message.correlation_id, position_data)
                    return
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            else:
                response_payload.update({"success": False, "error": "Unknown Error"})

            response = Message(
                bytes(json.dumps(response_payload), "utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
                correlation_id=message.correlation_id,
                content_type="application/json",
            )
            await self._send_bitmex_exchange.publish(
                response, routing_key=BITMEX_POSITION_TSL_ADDED_EVENT_KEY
            )

            message.ack()
