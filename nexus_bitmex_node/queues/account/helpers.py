import json
from json import JSONDecodeError

from aio_pika import IncomingMessage

from nexus_bitmex_node.event_bus import AccountEventEmitter
from nexus_bitmex_node.exceptions import WrongAccountError
from nexus_bitmex_node.exchange_account import ExchangeAccountManager


async def handle_create_account_message(message: IncomingMessage, account_manager: ExchangeAccountManager,
                                        event_emitter: AccountEventEmitter) -> str:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    api_key = data.get("apiKey")
    api_secret = data.get("apiSecret")
    account_id = data.get("accountId")

    await account_manager.connect(account_id, api_key, api_secret)
    await event_emitter.emit_account_created_event(account_id)

    return account_id


async def handle_update_account_message(message: IncomingMessage, account_manager: ExchangeAccountManager,
                                        event_emitter: AccountEventEmitter) -> str:
    try:
        data = json.loads(message.body.decode("utf-8"))
    except JSONDecodeError as err:
        raise err

    api_key = data.get("apiKey")
    api_secret = data.get("apiSecret")
    account_id = data.get("accountId")

    if account_manager.account is None or account_id != account_manager.account.account_id:
        raise WrongAccountError(account_id)

    await account_manager.connect(account_id, api_key, api_secret)
    await event_emitter.emit_account_updated_event(account_id)

    return account_id


async def handle_delete_account_message(message: IncomingMessage, account_manager: ExchangeAccountManager,
                                        event_emitter: AccountEventEmitter) -> str:
    try:
        data = json.loads(message.body)
    except JSONDecodeError as err:
        raise err

    account_id = data.get("accountId")

    if account_manager.account is None or account_id != account_manager.account.account_id:
        raise WrongAccountError(account_id)

    await account_manager.disconnect()
    await event_emitter.emit_account_deleted_event()

    return account_id
