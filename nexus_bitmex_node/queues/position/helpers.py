import json
from json import JSONDecodeError

from aio_pika import IncomingMessage

from nexus_bitmex_node.event_bus.position import PositionEventEmitter
from nexus_bitmex_node.exceptions import WrongOrderError


async def handle_close_position_message(message: IncomingMessage, event_emitter: PositionEventEmitter) -> bool:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    order_id = data.get("orderId")
    if not order_id:
        raise WrongOrderError(order_id)

    return data


async def handle_add_stop_to_position_message(message: IncomingMessage, event_emitter: PositionEventEmitter) -> str:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    pass


async def handle_add_tsl_to_position_message(message: IncomingMessage, event_emitter: PositionEventEmitter) -> str:
    try:
        data = json.loads(message.body)
    except JSONDecodeError as err:
        raise err

    pass
