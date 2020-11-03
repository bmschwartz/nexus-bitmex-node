import json
from json import JSONDecodeError

from aio_pika import IncomingMessage

from nexus_bitmex_node.event_bus import OrderEventEmitter
from nexus_bitmex_node.exceptions import WrongOrderError


async def handle_create_order_message(message: IncomingMessage, event_emitter: OrderEventEmitter) -> str:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    api_key = data.get("apiKey")
    api_secret = data.get("apiSecret")
    order_id = data.get("orderId")

    await event_emitter.emit_create_order_event(order_id, api_key, api_secret)

    return order_id


async def handle_update_order_message(message: IncomingMessage,
                                      event_emitter: OrderEventEmitter) -> str:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    order_id = data.get("orderId")

    # TODO: Check if I'm actually watching this
    # if order is None or order_id != order.order_id:
    #     raise WrongOrderError(order_id)

    await event_emitter.emit_update_order_event(order_id)

    return order_id


async def handle_delete_order_message(message: IncomingMessage, event_emitter: OrderEventEmitter) -> str:
    try:
        data = json.loads(message.body)
    except JSONDecodeError as err:
        raise err

    order_id = data.get("orderId")

    # TODO: Check if I'm actually watching this
    # if order is None or order_id != order.order_id:
    #     raise WrongOrderError(order_id)

    await event_emitter.emit_delete_order_event(order_id)

    return order_id
