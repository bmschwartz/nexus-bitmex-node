import json
from json import JSONDecodeError

from aio_pika import IncomingMessage

from nexus_bitmex_node.exceptions import WrongOrderError


async def handle_create_order_message(message: IncomingMessage) -> bool:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    orders = data.get("orders", {})
    if not orders or not orders.get("main", {}).get("id", None):
        raise WrongOrderError(None)

    return data


async def handle_update_order_message(message: IncomingMessage) -> str:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    order_id = data.get("orderId")

    return data


async def handle_cancel_order_message(message: IncomingMessage) -> str:
    try:
        data = json.loads(message.body)
    except JSONDecodeError as err:
        raise err

    if not data.get("orderId", None) or not data.get("accountId", None):
        raise WrongOrderError(None)

    return data
