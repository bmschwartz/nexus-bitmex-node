import json
import typing
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

from nexus_bitmex_node.event_bus import OrderEventEmitter, EventBus, OrderEventListener
from nexus_bitmex_node.exceptions import WrongOrderError
from nexus_bitmex_node.queues.order.helpers import (
    handle_create_order_message,
    handle_update_order_message,
)
from nexus_bitmex_node.queues.queue_manager import QueueManager, QUEUE_EXPIRATION_TIME
from nexus_bitmex_node.settings import BITMEX_EXCHANGE

from nexus_bitmex_node.queues.order.constants import (
    BINANCE_CREATE_ORDER_QUEUE,
    BINANCE_CREATE_ORDER_CMD_KEY,
    BINANCE_UPDATE_ORDER_QUEUE_PREFIX,
    BINANCE_UPDATE_ORDER_CMD_KEY_PREFIX,
    BINANCE_CANCEL_ORDER_QUEUE_PREFIX,
    BINANCE_CANCEL_ORDER_CMD_KEY_PREFIX,
    BINANCE_ORDER_CREATED_EVENT_KEY,
    BINANCE_ORDER_UPDATED_EVENT_KEY,
    BINANCE_ORDER_CANCELED_EVENT_KEY,
)


class OrderQueueManager(QueueManager, OrderEventEmitter, OrderEventListener):
    _recv_order_channel: Channel
    _send_order_channel: Channel

    _recv_binance_exchange: Exchange
    _send_binance_exchange: Exchange

    _create_order_queue: Queue
    _update_order_queue: Queue
    _delete_order_queue: Queue

    _update_order_routing_keys: typing.Dict[str, str]
    _delete_order_routing_keys: typing.Dict[str, str]

    def __init__(
        self,
        event_bus: EventBus,
        recv_connection: Connection,
        send_connection: Connection,
    ):
        QueueManager.__init__(self, recv_connection, send_connection)
        OrderEventEmitter.__init__(self, event_bus)

        self._create_order_consumer_tag = str(uuid4())
        self._update_order_consumer_tag = str(uuid4())
        self._delete_order_consumer_tag = str(uuid4())

    async def start(self):
        await super(OrderQueueManager, self).start()
        await self._listen_to_create_order_queue()

    async def register_listeners(self):
        self.register_create_order_listener()

    async def create_channels(self):
        self._recv_order_channel = await self.create_channel(self.recv_connection)
        self._send_order_channel = await self.create_channel(self.send_connection)
        await self._recv_order_channel.set_qos(prefetch_count=1)

    async def declare_queues(self):
        self._create_order_queue = await self._recv_order_channel.declare_queue(
            BINANCE_CREATE_ORDER_QUEUE, durable=True
        )

    async def declare_exchanges(self):
        self._recv_binance_exchange = await self._recv_order_channel.declare_exchange(
            BITMEX_EXCHANGE, type=ExchangeType.TOPIC, durable=True
        )

        self._send_binance_exchange = await self._send_order_channel.declare_exchange(
            BITMEX_EXCHANGE, type=ExchangeType.TOPIC, durable=True
        )

    async def _on_order_created(self, order_id: str) -> None:
        # TODO: Create queues to listen to order updates or delete message
        pass

    async def _on_order_deleted(self) -> None:
        # TODO: Unbind queues that listen to order updates or delete message
        pass

    async def _listen_to_create_order_queue(self):
        await self._create_order_queue.bind(
            self._recv_binance_exchange,
            BINANCE_CREATE_ORDER_CMD_KEY,
        )

        await self._create_order_queue.consume(
            self.on_create_order_message, consumer_tag=self._create_order_consumer_tag
        )

    async def _listen_to_update_order_queue(self, order_id: str) -> None:
        self._update_order_queue = await self._recv_order_channel.declare_queue(
            f"{BINANCE_UPDATE_ORDER_QUEUE_PREFIX}{order_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

        router_key = f"{BINANCE_UPDATE_ORDER_CMD_KEY_PREFIX}{order_id}"
        self._update_order_routing_keys[order_id] = router_key

        await self._update_order_queue.bind(self._recv_binance_exchange, router_key)
        await self._update_order_queue.consume(
            self.on_update_order_message, consumer_tag=self._update_order_consumer_tag
        )

    async def _listen_to_delete_order_queue(self, order_id: str) -> None:
        self._delete_order_queue = await self._recv_order_channel.declare_queue(
            f"{BINANCE_CANCEL_ORDER_QUEUE_PREFIX}{order_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

        router_key = f"{BINANCE_CANCEL_ORDER_CMD_KEY_PREFIX}{order_id}"
        self._delete_order_routing_keys[order_id] = router_key

        await self._delete_order_queue.bind(self._send_binance_exchange, router_key)
        await self._delete_order_queue.consume(
            self.on_delete_order_message, consumer_tag=self._delete_order_consumer_tag
        )

    async def on_create_order_message(self, message: IncomingMessage):
        async with message.process(ignore_processed=True):
            order_id = None
            response_payload: dict = {}

            try:
                order_id = await handle_create_order_message(message, self)
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            else:
                await self._on_order_created(order_id)
                response_payload.update({"success": True})

            if order_id:
                response_payload.update({"orderId": order_id})

            response = Message(
                bytes(json.dumps(response_payload), "utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
                correlation_id=message.correlation_id,
                content_type="application/json",
            )
            await self._send_binance_exchange.publish(
                response, routing_key=BINANCE_ORDER_CREATED_EVENT_KEY
            )
            message.ack()

    async def on_update_order_message(self, message: IncomingMessage):
        async with message.process():
            order_id = None

            response_payload: dict = {}

            try:
                order_id = await handle_update_order_message(message, self)
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            except WrongOrderError as e:
                order_id = e.order_id
                response_payload.update(
                    {"success": False, "error": "No matching order"}
                )
            else:
                response_payload.update({"success": True})

            if order_id:
                response_payload.update({"orderId": order_id})

            await self._send_binance_exchange.publish(
                Message(
                    bytes(json.dumps(response_payload), "utf-8"),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    correlation_id=message.correlation_id,
                    content_type="application/json",
                ),
                routing_key=BINANCE_ORDER_UPDATED_EVENT_KEY,
            )

    async def on_delete_order_message(self, message: IncomingMessage):
        async with message.process(ignore_processed=True):
            order_id = None

            response_payload: dict = {}

            try:
                pass
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            except WrongOrderError as e:
                order_id = e.order_id
                response_payload.update(
                    {"success": False, "error": "No matching order"}
                )
            else:
                message.ack()
                await self._on_order_deleted()
                response_payload.update({"success": True})

            if order_id:
                response_payload.update({"orderId": order_id})

            await self._send_binance_exchange.publish(
                Message(
                    bytes(json.dumps(response_payload), "utf-8"),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    correlation_id=message.correlation_id,
                    content_type="application/json",
                ),
                routing_key=BINANCE_ORDER_CANCELED_EVENT_KEY,
            )
