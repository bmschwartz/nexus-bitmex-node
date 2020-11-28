import json
from json import JSONDecodeError

from aio_pika import IncomingMessage

from nexus_bitmex_node.event_bus.position import PositionEventEmitter


async def handle_close_position_message(message: IncomingMessage) -> bool:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    return data


async def handle_add_stop_to_position_message(message: IncomingMessage) -> str:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    return data


async def handle_add_tsl_to_position_message(message: IncomingMessage) -> str:
    try:
        data = json.loads(message.body)
    except JSONDecodeError as err:
        raise err

    return data
