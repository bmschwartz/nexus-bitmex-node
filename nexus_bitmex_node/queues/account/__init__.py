import asyncio
import json
import datetime
from json import JSONDecodeError
from uuid import uuid4

import typing
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

from nexus_bitmex_node.event_bus import AccountEventEmitter, EventBus, AccountEventListener
from nexus_bitmex_node.exceptions import InvalidApiKeysError, WrongAccountError
from nexus_bitmex_node.exchange_account import ExchangeAccountManager
from nexus_bitmex_node.settings import BITMEX_EXCHANGE
from nexus_bitmex_node.queues.queue_manager import QueueManager, QUEUE_EXPIRATION_TIME
from .constants import (
    BITMEX_CREATE_ACCOUNT_QUEUE,
    BITMEX_CREATE_ACCOUNT_CMD_KEY,
    BITMEX_UPDATE_ACCOUNT_QUEUE_PREFIX,
    BITMEX_UPDATE_ACCOUNT_CMD_KEY_PREFIX,
    BITMEX_DELETE_ACCOUNT_CMD_KEY_PREFIX,
    BITMEX_DELETE_ACCOUNT_QUEUE_PREFIX,
    BITMEX_ACCOUNT_CREATED_EVENT_KEY,
    BITMEX_ACCOUNT_DELETED_EVENT_KEY,
    BITMEX_ACCOUNT_UPDATED_EVENT_KEY,
    BITMEX_HEARTBEAT_EVENT_KEY,
)
from .helpers import (
    handle_create_account_message,
    handle_update_account_message,
    handle_delete_account_message,
)


_HEARTBEAT_INTERVAL = 5


class AccountQueueManager(QueueManager, AccountEventEmitter, AccountEventListener):
    _exchange_account_manager: ExchangeAccountManager

    _recv_account_channel: Channel
    _send_account_channel: Channel

    _recv_bitmex_exchange: Exchange
    _send_bitmex_exchange: Exchange

    _create_account_queue: Queue
    _update_account_queue: Queue
    _delete_account_queue: Queue

    _update_account_routing_key: str
    _delete_account_routing_key: str

    _heartbeat_task: typing.Optional[asyncio.Task]

    def __init__(
            self,
            event_bus: EventBus,
            exchange_account_manager: ExchangeAccountManager,
            recv_connection: Connection,
            send_connection: Connection,
    ):
        QueueManager.__init__(self, recv_connection, send_connection)
        AccountEventEmitter.__init__(self, event_bus)
        AccountEventListener.__init__(self, event_bus)

        self._exchange_account_manager = exchange_account_manager

        self._create_account_consumer_tag = str(uuid4())
        self._update_account_consumer_tag = str(uuid4())
        self._delete_account_consumer_tag = str(uuid4())

        self._heartbeat_task = None

    def register_listeners(self):
        self.register_account_heartbeat_listener(self._send_heartbeat)

    async def start(self):
        await super(AccountQueueManager, self).start()
        await self._listen_to_create_account_queue()

    async def stop(self):
        await super(AccountQueueManager, self).stop()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def create_channels(self):
        self._recv_account_channel = await self.create_channel(self.recv_connection)
        self._send_account_channel = await self.create_channel(self.send_connection)
        await self._recv_account_channel.set_qos(prefetch_count=1)

    async def declare_queues(self):
        self._create_account_queue = await self._recv_account_channel.declare_queue(
            BITMEX_CREATE_ACCOUNT_QUEUE, durable=True
        )

    async def declare_exchanges(self):
        self._recv_bitmex_exchange = await self._recv_account_channel.declare_exchange(
            BITMEX_EXCHANGE, type=ExchangeType.TOPIC, durable=True
        )

        self._send_bitmex_exchange = await self._send_account_channel.declare_exchange(
            BITMEX_EXCHANGE, type=ExchangeType.TOPIC, durable=True
        )

    async def _send_heartbeat(self, interval) -> None:
        async def do_heartbeat():
            if not self._exchange_account_manager.account:
                return

            response_payload: dict = {
                "accountId": self._exchange_account_manager.account.account_id
            }
            expiration = datetime.datetime.now() + datetime.timedelta(seconds=20)

            await self._send_bitmex_exchange.publish(
                Message(
                    bytes(json.dumps(response_payload), "utf-8"),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    content_type="application/json",
                    expiration=expiration,
                ),
                routing_key=BITMEX_HEARTBEAT_EVENT_KEY,
            )

        while True:
            await asyncio.sleep(interval)
            await do_heartbeat()

    async def _on_account_created(self, account_id: str) -> None:
        await self._create_account_queue.unbind(self._recv_bitmex_exchange)
        await self._create_account_queue.cancel(self._create_account_consumer_tag)
        await self._listen_to_update_account_queue(account_id)
        await self._listen_to_delete_account_queue(account_id)

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._send_heartbeat(_HEARTBEAT_INTERVAL))

    async def _on_account_deleted(self) -> None:
        await self._update_account_queue.unbind(
            self._recv_bitmex_exchange, self._update_account_routing_key
        )
        await self._update_account_queue.cancel(self._update_account_consumer_tag)

        await self._delete_account_queue.unbind(
            self._recv_bitmex_exchange, self._delete_account_routing_key
        )
        await self._delete_account_queue.cancel(self._delete_account_consumer_tag)

        await self._listen_to_create_account_queue()

    async def _listen_to_create_account_queue(self):
        await self._create_account_queue.bind(
            self._recv_bitmex_exchange,
            BITMEX_CREATE_ACCOUNT_CMD_KEY,
        )

        await self._create_account_queue.consume(
            self.on_create_account_message,
            consumer_tag=self._create_account_consumer_tag,
        )

    async def _listen_to_update_account_queue(self, account_id: str) -> None:
        self._update_account_queue = await self._recv_account_channel.declare_queue(
            f"{BITMEX_UPDATE_ACCOUNT_QUEUE_PREFIX}{account_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

        self._update_account_routing_key = (
            f"{BITMEX_UPDATE_ACCOUNT_CMD_KEY_PREFIX}{account_id}"
        )
        await self._update_account_queue.bind(
            self._recv_bitmex_exchange, self._update_account_routing_key
        )

        await self._update_account_queue.consume(
            self.on_update_account_message,
            consumer_tag=self._update_account_consumer_tag,
        )

    async def _listen_to_delete_account_queue(self, account_id: str) -> None:
        self._delete_account_queue = await self._recv_account_channel.declare_queue(
            f"{BITMEX_DELETE_ACCOUNT_QUEUE_PREFIX}{account_id}",
            durable=True,
            arguments={"x-expires": QUEUE_EXPIRATION_TIME},
        )

        self._delete_account_routing_key = (
            f"{BITMEX_DELETE_ACCOUNT_CMD_KEY_PREFIX}{account_id}"
        )
        await self._delete_account_queue.bind(
            self._send_bitmex_exchange, self._delete_account_routing_key
        )

        await self._delete_account_queue.consume(
            self.on_delete_account_message,
            consumer_tag=self._delete_account_consumer_tag,
        )

    async def on_create_account_message(self, message: IncomingMessage):
        async with message.process(ignore_processed=True):
            if self._exchange_account_manager.account:
                message.reject(True)
                return

            account_id = None

            response_payload: dict = {}

            try:
                account_id = await handle_create_account_message(
                    message, self._exchange_account_manager, self
                )
            except InvalidApiKeysError as e:
                account_id = e.account_id
                response_payload.update({"success": False, "error": "Invalid API Keys"})
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            else:
                await self._on_account_created(account_id)
                response_payload.update({"success": True})

            if account_id:
                response_payload.update({"accountId": account_id})

            response = Message(
                bytes(json.dumps(response_payload), "utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
                correlation_id=message.correlation_id,
                content_type="application/json",
            )
            await self._send_bitmex_exchange.publish(
                response, routing_key=BITMEX_ACCOUNT_CREATED_EVENT_KEY
            )
            message.ack()

    async def on_update_account_message(self, message: IncomingMessage):
        async with message.process():
            account_id = None

            response_payload: dict = {}

            try:
                account_id = await handle_update_account_message(
                    message, self._exchange_account_manager, self
                )
            except InvalidApiKeysError as e:
                account_id = e.account_id
                response_payload.update({"success": False, "error": "Invalid API Keys"})
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            except WrongAccountError as e:
                account_id = e.account_id
                response_payload.update({"success": False, "error": "No matching account"})
            else:
                response_payload.update({"success": True})
            if account_id:
                response_payload.update({"accountId": account_id})

            await self._send_bitmex_exchange.publish(
                Message(
                    bytes(json.dumps(response_payload), "utf-8"),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    correlation_id=message.correlation_id,
                    content_type="application/json",
                ),
                routing_key=BITMEX_ACCOUNT_UPDATED_EVENT_KEY,
            )

    async def on_delete_account_message(self, message: IncomingMessage):
        async with message.process(ignore_processed=True):
            account_id = None

            response_payload: dict = {}

            try:
                account_id = await handle_delete_account_message(
                    message, self._exchange_account_manager, self
                )
            except JSONDecodeError:
                response_payload.update({"success": False, "error": "Invalid Message"})
            except WrongAccountError as e:
                account_id = e.account_id
                response_payload.update({"success": False, "error": "No matching account"})
            else:
                message.ack()
                await self._on_account_deleted()
                response_payload.update({"success": True})

            if account_id:
                response_payload.update({"accountId": account_id})

            await self._send_bitmex_exchange.publish(
                Message(
                    bytes(json.dumps(response_payload), "utf-8"),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    correlation_id=message.correlation_id,
                    content_type="application/json",
                ),
                routing_key=BITMEX_ACCOUNT_DELETED_EVENT_KEY,
            )
