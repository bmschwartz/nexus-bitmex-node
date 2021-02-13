import json
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

from nexus_bitmex_node.event_bus import OrderEventEmitter, EventBus, OrderEventListener, AccountEventListener, \
    ExchangeEventListener
from nexus_bitmex_node.exceptions import WrongOrderError
from nexus_bitmex_node.queues.order.helpers import (
    handle_create_order_message,
    handle_update_order_message,
)
from nexus_bitmex_node.queues.queue_manager import QueueManager, QUEUE_EXPIRATION_TIME
from nexus_bitmex_node.queues.utils import cleanup_queue
from nexus_bitmex_node.settings import BITMEX_EXCHANGE

from nexus_bitmex_node.queues.order.constants import (
    BITMEX_CREATE_ORDER_CMD_PREFIX,
    BITMEX_CREATE_ORDER_QUEUE_PREFIX,
    BITMEX_UPDATE_ORDER_QUEUE_PREFIX,
    BITMEX_UPDATE_ORDER_CMD_KEY_PREFIX,
    BITMEX_CANCEL_ORDER_QUEUE_PREFIX,
    BITMEX_CANCEL_ORDER_CMD_KEY_PREFIX,
    BITMEX_ORDER_CREATED_EVENT_KEY,
    BITMEX_ORDER_UPDATED_EVENT_KEY,
    BITMEX_ORDER_CANCELED_EVENT_KEY,
)


class OrderQueueManager(
    QueueManager,
    OrderEventEmitter,
    OrderEventListener,
    AccountEventListener,
    ExchangeEventListener
):
    _recv_order_channel: Channel
    _send_order_channel: Channel

    _recv_bitmex_exchange: Exchange
    _send_bitmex_exchange: Exchange

    _create_order_queue: Queue
    _update_order_queue: Queue
    _cancel_order_queue: Queue

    _create_order_routing_key: str
    _update_order_routing_key: str
    _cancel_order_routing_key: str

    def __init__(
        self,
        event_bus: EventBus,
        recv_connection: Connection,
        send_connection: Connection,
    ):
        QueueManager.__init__(self, recv_connection, send_connection)
        OrderEventEmitter.__init__(self, event_bus)
        AccountEventListener.__init__(self, event_bus)
        ExchangeEventListener.__init__(self, event_bus)

        self._create_order_consumer_tag = str(uuid4())
        self._update_order_consumer_tag = str(uuid4())
        self._cancel_order_consumer_tag = str(uuid4())

    async def start(self):
        await super(OrderQueueManager, self).start()

    async def declare_queues(self):
        # Can't declare any queues on startup because we don't have a linked account
        pass

    async def create_channels(self):
        self._recv_order_channel = await self.create_channel(self.recv_connection)
        self._send_order_channel = await self.create_channel(self.send_connection)
        await self._recv_order_channel.set_qos(prefetch_count=1)

    def register_listeners(self):
        loop = asyncio.get_event_loop()
        self.register_account_created_listener(self.listen_to_order_queues, loop)
        self.register_account_deleted_listener(self.stop_listening_to_order_queues, loop)
        self.register_order_created_listener(self._on_order_created, loop)
        self.register_order_updated_listener(self._on_order_updated, loop)
        self.register_trades_updated_listener(self._on_trades_updated, loop)

    async def declare_exchanges(self):
        self._recv_bitmex_exchange = await self._recv_order_channel.declare_exchange(
            BITMEX_EXCHANGE, type=ExchangeType.TOPIC, durable=True
        )

        self._send_bitmex_exchange = await self._send_order_channel.declare_exchange(
            BITMEX_EXCHANGE, type=ExchangeType.TOPIC, durable=True
        )

    async def _on_order_created(self, message_id: str, orders: typing.Dict,
                                errors: typing.Dict[str, str] = None) -> None:
        def create_order_data(order_data):
            order = order_data["info"]
            if not order["clOrdID"]:
                return {}
            order_qty = order["orderQty"]
            leaves_qty = order["leavesQty"]
            filled_qty = order_qty - leaves_qty if order_qty and leaves_qty else None
            return {
                "orderId": order["orderID"],
                "status": order["ordStatus"],
                "clOrderId": '_'.join(order["clOrdID"].split("_")[:2]),
                "clOrderLinkId": order["clOrdLinkID"],
                "orderQty": order_qty,
                "filledQty": filled_qty,
                "price": order["price"],
                "avgPrice": order["avgPx"],
                "stopPrice": order["stopPx"],
                "pegOffsetValue": order["pegOffsetValue"],
                "timestamp": order["timestamp"],
            }

        orders_data = None

        if orders:
            orders_data = {
                "main": create_order_data(orders["main"]),
            }

            stop_order = orders.get("stop", {})
            tsl_order = orders.get("tsl", {})

            if stop_order:
                orders_data["stop"] = create_order_data(stop_order)
            if tsl_order:
                orders_data["tsl"] = create_order_data(tsl_order)

        response_payload: dict = {
            "orders": orders_data,
            "success": not errors,
            "errors": errors,
        }

        response = Message(
            bytes(json.dumps(response_payload), "utf-8"),
            delivery_mode=DeliveryMode.PERSISTENT,
            correlation_id=message_id,
            content_type="application/json",
        )
        await self._send_bitmex_exchange.publish(
            response, routing_key=BITMEX_ORDER_CREATED_EVENT_KEY
        )

    async def _on_order_updated(self, order_update: typing.Dict) -> None:
        order = order_update["info"]
        if not order["clOrdID"]:
            return

        order_qty = order["orderQty"]
        leaves_qty = order["leavesQty"] or 0
        filled_qty = order_qty - leaves_qty if order_qty else None

        order_data = {
            "orderId": order["orderID"],
            "status": order["ordStatus"],
            "clOrderId": '_'.join(order["clOrdID"].split("_")[:2]),
            "clOrderLinkId": order["clOrdLinkID"],
            "orderQty": order_qty,
            "filledQty": filled_qty,
            "price": order["price"],
            "avgPrice": order["avgPx"],
            "stopPrice": order["stopPx"],
            "pegOffsetValue": order["pegOffsetValue"],
            "timestamp": order["timestamp"],
        }
        response_payload: dict = {
            "order": order_data,
        }

        response = Message(
            bytes(json.dumps(response_payload), "utf-8"),
            delivery_mode=DeliveryMode.PERSISTENT,
            content_type="application/json",
        )
        await self._send_bitmex_exchange.publish(
            response, routing_key=BITMEX_ORDER_UPDATED_EVENT_KEY
        )

    async def _on_order_canceled(self, order_id: str) -> None:
        # TODO: Do something now that an order has been canceled
        pass

    async def _on_trades_updated(self, account_id: str, orders_data: typing.List) -> None:
        pass

    async def listen_to_order_queues(self, account_id: str):
        await self.stop_listening_to_order_queues()
        await self._declare_order_queues(account_id)
        self._set_order_routing_keys(account_id)
        await self._bind_order_queues()
        await self._attach_consumers()

    async def stop_listening_to_order_queues(self):
        if getattr(self, "_create_order_queue", None):
            await cleanup_queue(self._create_order_queue, self._recv_bitmex_exchange)
            setattr(self, "_create_order_queue", None)

        if getattr(self, "_update_order_queue", None):
            await cleanup_queue(self._update_order_queue, self._recv_bitmex_exchange)
            setattr(self, "_update_order_queue", None)

        if getattr(self, "_cancel_order_queue", None):
            await cleanup_queue(self._cancel_order_queue, self._recv_bitmex_exchange)
            setattr(self, "_cancel_order_queue", None)

    def _set_order_routing_keys(self, account_id: str):
        self._create_order_routing_key = f"{BITMEX_CREATE_ORDER_CMD_PREFIX}{account_id}"
        self._update_order_routing_key = f"{BITMEX_UPDATE_ORDER_CMD_KEY_PREFIX}{account_id}"
        self._cancel_order_routing_key = f"{BITMEX_CANCEL_ORDER_CMD_KEY_PREFIX}{account_id}"

    async def _declare_order_queues(self, account_id: str):
        # Declare queues
        self._create_order_queue = await self._recv_order_channel.declare_queue(
            f"{BITMEX_CREATE_ORDER_QUEUE_PREFIX}{account_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

        self._update_order_queue = await self._recv_order_channel.declare_queue(
            f"{BITMEX_UPDATE_ORDER_QUEUE_PREFIX}{account_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

        self._cancel_order_queue = await self._recv_order_channel.declare_queue(
            f"{BITMEX_CANCEL_ORDER_QUEUE_PREFIX}{account_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

    async def _bind_order_queues(self):
        await self._create_order_queue.bind(
            self._send_bitmex_exchange, self._create_order_routing_key
        )
        await self._update_order_queue.bind(
            self._send_bitmex_exchange, self._update_order_routing_key
        )
        await self._cancel_order_queue.bind(
            self._send_bitmex_exchange, self._cancel_order_routing_key
        )

    async def _attach_consumers(self):
        await self._create_order_queue.consume(
            self.on_create_order_message, consumer_tag=self._create_order_consumer_tag
        )
        await self._update_order_queue.consume(
            self.on_update_order_message, consumer_tag=self._update_order_consumer_tag
        )
        await self._cancel_order_queue.consume(
            self.on_cancel_order_message, consumer_tag=self._cancel_order_consumer_tag
        )

    async def on_create_order_message(self, message: IncomingMessage):
        async with message.process(ignore_processed=True):
            order_id = None
            response_payload: dict = {}

            try:
                order_data = await handle_create_order_message(message)
                if order_data:
                    message.ack()
                    await self.emit_create_order_event(message.correlation_id, order_data)
                    return
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            except WrongOrderError:
                response_payload.update({"success": False, "error": "Bad Order ID"})
            else:
                response_payload.update({"success": False, "error": "Unknown Error"})

            response = Message(
                bytes(json.dumps(response_payload), "utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
                correlation_id=message.correlation_id,
                content_type="application/json",
            )
            await self._send_bitmex_exchange.publish(
                response, routing_key=BITMEX_ORDER_CREATED_EVENT_KEY
            )

            message.ack()

    async def on_update_order_message(self, message: IncomingMessage):
        async with message.process():
            order_id = None

            response_payload: dict = {}

            try:
                order_data = await handle_update_order_message(message)
                if order_data:
                    message.ack()
                    await self.emit_update_order_event(message.correlation_id, order_data)
                    return
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            except WrongOrderError as e:
                order_id = e.order_id
                response_payload.update(
                    {"success": False, "error": "No matching order"}
                )
            else:
                response_payload.update({"success": False, "error": "Unknown Error"})

            await self._send_bitmex_exchange.publish(
                Message(
                    bytes(json.dumps(response_payload), "utf-8"),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    correlation_id=message.correlation_id,
                    content_type="application/json",
                ),
                routing_key=BITMEX_ORDER_UPDATED_EVENT_KEY,
            )

            message.ack()

    async def on_cancel_order_message(self, message: IncomingMessage):
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
                response_payload.update({"success": True})

            if order_id:
                await self._on_order_canceled(order_id)
                response_payload.update({"orderId": order_id})

            await self._send_bitmex_exchange.publish(
                Message(
                    bytes(json.dumps(response_payload), "utf-8"),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    correlation_id=message.correlation_id,
                    content_type="application/json",
                ),
                routing_key=BITMEX_ORDER_CANCELED_EVENT_KEY,
            )

            message.ack()
