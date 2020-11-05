import aio_pika
from aio_pika import Connection
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from uvicorn.loops import asyncio as uv_asyncio

from nexus_bitmex_node import settings
from nexus_bitmex_node.event_bus import event_bus
from nexus_bitmex_node.exchange_account import ExchangeAccountManager
from nexus_bitmex_node.queues import AccountQueueManager, OrderQueueManager
from nexus_bitmex_node.storage import data_store
from nexus_bitmex_node.settings import REDIS_URL, AMQP_URL

exchange_account_manager: ExchangeAccountManager
account_queue_manager: AccountQueueManager
order_queue_manager: OrderQueueManager

uv_asyncio.asyncio_setup()


def status(request: Request) -> JSONResponse:
    return JSONResponse()


async def on_start():
    await setup_queue_managers()
    await data_store.start(REDIS_URL)


async def on_shutdown():
    if account_queue_manager:
        await account_queue_manager.stop()

    if order_queue_manager:
        await order_queue_manager.stop()

    if data_store:
        await data_store.stop()

    if exchange_account_manager:
        await exchange_account_manager.disconnect()


async def setup_queue_managers():
    global exchange_account_manager
    global account_queue_manager
    global order_queue_manager

    recv_connection: Connection = await aio_pika.connect_robust(AMQP_URL)
    send_connection: Connection = await aio_pika.connect_robust(AMQP_URL)

    exchange_account_manager = ExchangeAccountManager(event_bus)

    account_queue_manager = AccountQueueManager(
        event_bus, exchange_account_manager, recv_connection, send_connection
    )
    await account_queue_manager.start()

    order_queue_manager = OrderQueueManager(event_bus, recv_connection, send_connection)
    await order_queue_manager.start()


routes = [
    Route("/status", status),
]

app = Starlette(
    debug=settings.IS_DEBUG,
    routes=routes,
    on_startup=[on_start],
    on_shutdown=[on_shutdown],
)
