import asyncio
import threading
import uuid
import typing
from datetime import datetime

import ccxtpro
from ccxtpro.base import AuthenticationError as ClientAuthenticationError

from nexus_bitmex_node import settings
from nexus_bitmex_node.bitmex import bitmex_manager, BitmexManager
from nexus_bitmex_node.event_bus import (
    OrderEventListener, OrderEventEmitter,
    EventBus, AccountEventEmitter, ExchangeEventEmitter,
)
from nexus_bitmex_node.exceptions import InvalidApiKeysError
from nexus_bitmex_node.models.order import BitmexOrder, create_order
from nexus_bitmex_node.settings import ServerMode
from nexus_bitmex_node.storage import DataStore


class ExchangeAccount(AccountEventEmitter, ExchangeEventEmitter, OrderEventListener, OrderEventEmitter):
    def __init__(self, bus: EventBus, data_store: DataStore, account_id: str, api_key: str, api_secret: str):
        """
        Connect Bitmex exchange account
        :param account_id
        :param api_key:
        :param api_secret:
        :raises:
            InvalidApiKeys: Raised when either the api_key or api_secret are invalid
        """
        AccountEventEmitter.__init__(self, event_bus=bus)
        OrderEventListener.__init__(self, event_bus=bus)

        if any([not api_key, not api_secret]):
            raise InvalidApiKeysError(account_id)
        self.account_id = account_id
        self._data_store = data_store
        self._api_key = api_key
        self._api_secret = api_secret
        self._client: typing.Optional[ccxtpro.bitmex] = None
        self._websocket_stream_ids: typing.List[uuid.UUID] = []

    async def start(self):
        await self._connect_client()
        await self._init_tickers()
        await self._connect_to_socket_stream()

    async def disconnect(self):
        if self._client:
            await self._client.close()
            self._client = None

        bitmex_manager.stop_streams()

    def register_listeners(self):
        self.register_create_order_listener(self._on_create_order)

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
            margins = await self._client.fetch_balance()
            positions = await self._client.fetch_positions()
            orders = await self._client.fetch_orders(limit=500, params={"reverse": True})

            await self.emit_margins_updated_event(self.account_id, margins)
            await self.emit_my_trades_updated_event(self.account_id, orders)
            await self.emit_positions_updated_event(self.account_id, positions)
        except ClientAuthenticationError:
            print(f"invalid creds {self._api_key} {self._api_secret}")
            raise InvalidApiKeysError(self.account_id)

    async def _connect_to_socket_stream(self):
        await self._client.watch_balance()
        await self._client.watch_my_trades()
        await self._client.watch_positions()
        await self._client.watch_instruments()

        worker_thread = threading.Thread(
            target=asyncio.run,
            args=(bitmex_manager.watch_streams(self.account_id, self._client),)
        )
        worker_thread.start()

    async def _init_tickers(self):
        data = await self._client.fetch_tickers()
        tickers: typing.Dict = {}
        for ticker in data.values():
            info = ticker.get("info")
            if not info.get("state") in ("Open",):
                continue
            symbol = info.get("symbol")
            tickers[symbol] = info
        await self.emit_ticker_updated_event(self.account_id, tickers)

    async def _on_create_order(self, message_id: str, order_data: dict):
        order: BitmexOrder = create_order(order_data)
        ticker = await self._data_store.get_ticker(self.account_id, order.symbol)

        # currency = ticker.get("underlying")
        # self._client.safe_market(order.symbol)
        # safe = self._client.safe_currency(currency)
        # TODO: Fix this
        currency = "BTC"

        margin = await self._data_store.get_margin(self.account_id, "XBt")
        margin_balance = margin.get("available", 0)

        try:
            order_data = await BitmexManager.place_order(self._client, order, ticker, margin_balance)
            await self.emit_order_created_event(message_id, order=order_data)
        except ccxtpro.base.errors.BaseError as e:
            error = e.args
            await self.emit_order_created_event(message_id, order=None, error=e)
        except Exception as e:
            await self.emit_order_created_event(message_id, order=None, error="Unknown Error")


class ExchangeAccountManager:
    def __init__(self, bus: EventBus, data_store: DataStore):
        self._event_bus = bus
        self._data_store = data_store
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
            self._account = ExchangeAccount(self._event_bus, self._data_store, account_id, api_key, api_secret)
            await self._account.start()
        except InvalidApiKeysError as e:
            await self.disconnect()
            raise e

    async def disconnect(self):
        if self._account:
            await self._account.disconnect()
        self._account = None

