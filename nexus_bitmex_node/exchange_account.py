import uuid
import typing
from datetime import datetime

import ccxt.async_support as ccxt

from nexus_bitmex_node.event_bus import EventBus, AccountEventEmitter
from nexus_bitmex_node.event_bus.balance import BalanceEventEmitter
from nexus_bitmex_node.exceptions import InvalidApiKeysError


class ExchangeAccount(AccountEventEmitter, BalanceEventEmitter):
    def __init__(self, bus: EventBus, account_id: str, api_key: str, api_secret: str):
        """
        Connect Bitmex exchange account
        :param account_id
        :param api_key:
        :param api_secret:
        :raises:
            InvalidApiKeys: Raised when either the api_key or api_secret are invalid
        """
        BalanceEventEmitter.__init__(self, bus)

        if any([not api_key, not api_secret]):
            raise InvalidApiKeysError(account_id)
        self.account_id = account_id
        self._api_key = api_key
        self._api_secret = api_secret
        self._client: typing.Optional[ccxt.Exchange] = None
        self._websocket_manager = None
        self._websocket_stream_ids: typing.List[uuid.UUID] = []

    async def start(self):
        await self._connect_client()
        self._connect_to_socket_stream()

    async def disconnect(self):
        if self._client:
            await self._client.close()
            self._client = None

        if self._websocket_manager:
            self._websocket_manager.stop_manager_with_all_streams()
            self._websocket_manager = None

    async def _connect_client(self):
        # self._client = ccxt.bitmex(
        #     {
        #         "apiKey": self._api_key,
        #         "secret": self._api_secret,
        #         "timeout": 30000,
        #         "enableRateLimit": True,
        #     }
        # )
        # try:
        #     self._client.check_required_credentials()
        #     balances = await self._client.fetch_balance()
        # except ClientAuthenticationError:
        #     raise InvalidApiKeysError(self.account_id)
        #
        # await self._store_balances(balances["info"]["balances"])
        pass

    def _connect_to_socket_stream(self):
        # self._websocket_manager = BitmexWebSocketApiManager(
        #     exchange="bitmex.com", throw_exception_if_unrepairable=True
        # )
        # self._create_streams()

        # worker_thread = threading.Thread(
        #     target=asyncio.run,
        #     args=(
        #         bitmex_manager.handle_stream_data_from_stream_buffer(
        #             self._websocket_manager
        #         ),
        #     ),
        # )
        # worker_thread.start()
        pass

    def _create_streams(self):
        user_stream_id = self._websocket_manager.create_stream(
            "arr", "!userData", api_key=self._api_key, api_secret=self._api_secret
        )
        if user_stream_id:
            self._websocket_stream_ids.append(user_stream_id)

        ticker_stream_id = self._websocket_manager.create_stream("arr", "!miniTicker")
        if ticker_stream_id:
            self._websocket_stream_ids.append(ticker_stream_id)

    async def _store_balances(self, balances):
        def transform_balance(balance):
            free = balance["free"]
            locked = balance["locked"]
            return {"free": free, "locked": locked}

        transformed = {
            balance["asset"]: transform_balance(balance) for balance in balances
        }
        await self.emit_balances_updated_event(self.account_id, transformed)


class ExchangeAccountManager:
    def __init__(self, bus: EventBus):
        self._event_bus = bus
        self._last_update: datetime = datetime.now()
        self._account: typing.Optional[ExchangeAccount] = None

    @property
    def account(self) -> typing.Optional[ExchangeAccount]:
        return self._account

    async def connect(self, account_id: str, api_key: str, api_secret: str):
        """
        Connect a Bitmex account with the given API keys
        :param account_id: Identifies the user
        :param api_key: Bitmex API Key
        :param api_secret: Bitmex API Secret
        """
        await self.disconnect()

        try:
            self._account = ExchangeAccount( self._event_bus, account_id, api_key, api_secret)
            await self._account.start()
        except InvalidApiKeysError as e:
            await self.disconnect()
            raise e

    async def disconnect(self):
        if self._account:
            await self._account.disconnect()
        self._account = None

