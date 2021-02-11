import asyncio
import uuid
import typing
from datetime import datetime

import ccxtpro
from ccxt.base.errors import BadRequest, BaseError
from ccxtpro.base import AuthenticationError as ClientAuthenticationError

from nexus_bitmex_node import settings
from nexus_bitmex_node.bitmex import bitmex_manager, BitmexManager
from nexus_bitmex_node.event_bus import (
    OrderEventListener, OrderEventEmitter,
    EventBus, AccountEventEmitter, ExchangeEventEmitter, PositionEventEmitter, PositionEventListener,
)
from nexus_bitmex_node.exceptions import InvalidApiKeysError
from nexus_bitmex_node.models.order import BitmexOrder, create_order, StopTriggerType, OrderSide
from nexus_bitmex_node.models.position import BitmexPosition
from nexus_bitmex_node.models.symbol import BitmexSymbol, create_symbol
from nexus_bitmex_node.settings import ServerMode
from nexus_bitmex_node.storage import DataStore


class ExchangeAccount(
    AccountEventEmitter,
    ExchangeEventEmitter,
    OrderEventListener,
    OrderEventEmitter,
    PositionEventListener,
    PositionEventEmitter
):
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
        loop = asyncio.get_event_loop()
        self.register_create_order_listener(self._on_create_order, loop)
        self.register_close_position_listener(self._on_close_position, loop)
        self.register_add_stop_to_position_listener(self._on_add_stop_to_position, loop)
        self.register_add_tsl_to_position_listener(self._on_add_tsl_to_position, loop)

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
            [await self.emit_order_updated_event(order) for order in orders]
        except ClientAuthenticationError:
            print(f"invalid creds {self._api_key} {self._api_secret}")
            raise InvalidApiKeysError(self.account_id)

    async def _connect_to_socket_stream(self):
        bitmex_manager.start_streams()

        asyncio.ensure_future(bitmex_manager.watch_my_trades_stream(self.account_id, self._client))
        asyncio.ensure_future(bitmex_manager.watch_positions_stream(self.account_id, self._client))
        asyncio.ensure_future(bitmex_manager.watch_tickers_stream(self.account_id, self._client))
        asyncio.ensure_future(bitmex_manager.watch_balance_stream(self.account_id, self._client))
        asyncio.ensure_future(bitmex_manager.watch_orders_stream(self.account_id, self._client))

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
        orders = order_data["orders"]

        main_order: BitmexOrder = create_order(orders[0])
        stop_order: typing.Optional[BitmexOrder] = None
        tsl_order: typing.Optional[BitmexOrder] = None

        for order in orders:
            cl_order_id = order.get("clOrderId")
            if "stop" in cl_order_id:
                stop_order = create_order(order)
            elif "tsl" in cl_order_id:
                tsl_order = create_order(order)

        ticker = await self._data_store.get_ticker(self.account_id, main_order.symbol)

        # currency = ticker.get("underlying")
        # self._client.safe_market(order.symbol)
        # safe = self._client.safe_currency(currency)
        # TODO: Fix this
        currency = "BTC"

        margin = await self._data_store.get_margin(self.account_id, "XBt")
        margin_balance = margin.get("available", 0)

        order_results = {}
        try:
            main_order_result = await BitmexManager.place_order(self._client, main_order, ticker, margin_balance)
            order_results["main"] = main_order_result
            if stop_order:
                stop_order_result = await BitmexManager.place_stop_order(self._client, stop_order,
                                                                         main_order_result["amount"], ticker)
                order_results["stop"] = stop_order_result
            if tsl_order:
                tsl_order_result = await BitmexManager.place_tsl_order(self._client, tsl_order,
                                                                       main_order_result["amount"], ticker)
                order_results["tsl"] = tsl_order_result
            await self.emit_order_created_event(message_id, orders=order_results)
        except (BaseError, BadRequest) as e:
            error = e.args
            await self.emit_order_created_event(message_id, orders=None, error=e)
        except Exception as e:
            await self.emit_order_created_event(message_id, orders=None, error="Unknown Error")

    async def _on_close_position(self, message_id: str, data: typing.Dict):
        order: BitmexOrder = create_order(data)

        position: typing.Optional[BitmexPosition] = await self._data_store.get_position(self.account_id, order.symbol)
        if not position:
            await self.emit_position_closed_event(message_id, None, "Position not found")
            return

        try:
            close_order = await BitmexManager.close_position(self._client, order, position)
            await self.emit_position_closed_event(message_id, close_order)
        except (BaseError, BadRequest) as e:
            error = e.args
            await self.emit_position_closed_event(message_id, None, error=e)
        except Exception as e:
            await self.emit_position_closed_event(message_id, None, error="Unknown Error")

    async def _on_add_stop_to_position(self, message_id: str, data: typing.Dict):
        raw_symbol: str = str(data.get("symbol"))
        stop_price: float = data.get("stopPrice", -1)
        trigger_price_type: StopTriggerType = data.get("stopTriggerPriceType", StopTriggerType.LAST_PRICE.value)

        if not raw_symbol:
            await self.emit_added_stop_to_position_event(message_id, None, "Symbol required")
            return

        stored_data: typing.Optional[dict] = await self._data_store.get_ticker(self.account_id, raw_symbol)
        if not stored_data:
            await self.emit_added_stop_to_position_event(message_id, None, "Symbol not found")
            return

        symbol: BitmexSymbol = create_symbol(stored_data)

        if not stop_price or stop_price < 0:
            await self.emit_added_stop_to_position_event(message_id, None, "Stop price is required")
            return

        position: typing.Optional[BitmexPosition] = await self._data_store.get_position(self.account_id, symbol.symbol)
        if not position:
            await self.emit_added_stop_to_position_event(message_id, None, "Position not found")
            return

        try:
            stop_order = await BitmexManager.add_stop_to_position(
                self._client, symbol, position, stop_price, trigger_price_type)
            await self.emit_added_stop_to_position_event(message_id, stop_order)
        except (BaseError, BadRequest) as e:
            error = e.args
            await self.emit_added_stop_to_position_event(message_id, stop_order=None, error=e)
        except Exception as e:
            error = str(e) or "Unknown Error"
            await self.emit_added_stop_to_position_event(message_id, stop_order=None, error=error)

    async def _on_add_tsl_to_position(self, message_id: str, data: typing.Dict):
        raw_symbol: str = str(data.get("symbol"))
        tsl_percent: typing.Optional[float] = data.get("tslPercent")
        trigger_price_type: StopTriggerType = data.get("stopTriggerPriceType", StopTriggerType.LAST_PRICE.value)

        if not tsl_percent:
            await self.emit_added_stop_to_position_event(message_id, None, "Trailing Stop Percent required")
            return

        if not raw_symbol:
            await self.emit_added_stop_to_position_event(message_id, None, "Symbol required")
            return

        stored_data: typing.Optional[dict] = await self._data_store.get_ticker(self.account_id, raw_symbol)
        if not stored_data:
            await self.emit_added_stop_to_position_event(message_id, None, "Symbol not found")
            return

        symbol: BitmexSymbol = create_symbol(stored_data)

        position: typing.Optional[BitmexPosition] = await self._data_store.get_position(self.account_id, symbol.symbol)
        if not position:
            await self.emit_added_stop_to_position_event(message_id, None, "Position not found")
            return

        try:
            tsl_order = await BitmexManager.add_tsl_to_position(
                self._client, symbol, position, tsl_percent, trigger_price_type)
            await self.emit_added_tsl_to_position_event(message_id, tsl_order)
        except (BaseError, BadRequest) as e:
            error = e.args
            await self.emit_added_tsl_to_position_event(message_id, tsl_order=None, error=e)
        except Exception as e:
            error = str(e) or "Unknown Error"
            await self.emit_added_tsl_to_position_event(message_id, tsl_order=None, error=error)


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

