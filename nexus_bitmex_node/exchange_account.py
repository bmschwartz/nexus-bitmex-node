import asyncio
import threading
import uuid
import typing
from datetime import datetime

import ccxtpro
from ccxtpro.base import AuthenticationError as ClientAuthenticationError

from nexus_bitmex_node import settings
from nexus_bitmex_node.bitmex import bitmex_manager
from nexus_bitmex_node.event_bus import EventBus, AccountEventEmitter
from nexus_bitmex_node.exceptions import InvalidApiKeysError
from nexus_bitmex_node.settings import ServerMode


class ExchangeAccount(AccountEventEmitter):
    def __init__(self, bus: EventBus, account_id: str, api_key: str, api_secret: str):
        """
        Connect Bitmex exchange account
        :param account_id
        :param api_key:
        :param api_secret:
        :raises:
            InvalidApiKeys: Raised when either the api_key or api_secret are invalid
        """
        AccountEventEmitter.__init__(self, event_bus=bus)

        if any([not api_key, not api_secret]):
            raise InvalidApiKeysError(account_id)
        self.account_id = account_id
        self._api_key = api_key
        self._api_secret = api_secret
        self._client: typing.Optional[ccxtpro.bitmex] = None
        self._websocket_stream_ids: typing.List[uuid.UUID] = []

    async def start(self):
        await self._connect_client()
        await self._connect_to_socket_stream()

    async def disconnect(self):
        if self._client:
            await self._client.close()
            self._client = None

        bitmex_manager.stop_streams()

    async def _connect_client(self):
        self._client = ccxtpro.bitmex(
            {
                "apiKey": self._api_key,
                "secret": self._api_secret,
                "timeout": 30000,
                "enableRateLimit": True,
            }
        )

        if not settings.SERVER_MODE == ServerMode.PROD:
            self._client.set_sandbox_mode(True)

        try:
            balances = await self._client.fetch_balance()
        except ClientAuthenticationError:
            print(f"invalid creds {self._api_key} {self._api_secret}")
            raise InvalidApiKeysError(self.account_id)

    async def _connect_to_socket_stream(self):
        await self._client.watch_balance()
        await self._client.watch_my_trades()
        await self._client.watch_positions()

        worker_thread = threading.Thread(
            target=asyncio.run,
            args=(bitmex_manager.watch_streams(self.account_id, self._client),)
        )
        worker_thread.start()


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

