import json
import logging
import typing
from uuid import uuid4
from datetime import datetime

import ccxtpro
import watchtower
from ccxt import (
    AuthenticationError, PermissionDenied, ArgumentsRequired,
    InsufficientFunds, InvalidOrder, OrderNotFound,
)
from tenacity import (
    wait_random,
    AsyncRetrying,
    stop_after_attempt,
    retry_unless_exception_type,
)

from nexus_bitmex_node import settings
from nexus_bitmex_node.event_bus import (
    EventBus,
    event_bus,
    ExchangeEventEmitter, OrderEventEmitter,
)
from nexus_bitmex_node.models.order import BitmexOrder, OrderSide, OrderType, StopTriggerType
from nexus_bitmex_node.models.position import BitmexPosition
from nexus_bitmex_node.models.symbol import BitmexSymbol, create_symbol

FATAL_ORDER_EXCEPTIONS = (
    AuthenticationError,
    PermissionDenied,
    ArgumentsRequired,
    InsufficientFunds,
    InvalidOrder,
    OrderNotFound,
)

logger = logging.getLogger(__name__)
logger.addHandler(watchtower.CloudWatchLogHandler(log_group="nexus-bitmex-node", stream_name=settings.app_env))


def _set_leverage_successfully(retry_state):
    return retry_state.get("leverage", None) is not None


class BitmexManager(ExchangeEventEmitter, OrderEventEmitter):
    _symbol_data: dict
    _client: ccxtpro.bitmex
    _client_id: str
    _watching_streams: bool
    _orders_cache: dict
    _positions_cache: dict

    def __init__(self, bus: EventBus):
        ExchangeEventEmitter.__init__(self, bus)
        self._watching_streams = False
        self._symbol_data = {}
        self._orders_cache = {}
        self._positions_cache = {}

    def start_streams(self):
        self._watching_streams = True

    def stop_streams(self):
        self._watching_streams = False

    @staticmethod
    async def place_order(client: ccxtpro.bitmex, order: BitmexOrder, ticker, margin):
        async def execute_order():
            async for attempt in AsyncRetrying(reraise=True,
                                               stop=stop_after_attempt(3),
                                               retry=retry_unless_exception_type(FATAL_ORDER_EXCEPTIONS),
                                               wait=wait_random(min=5, max=20)):
                with attempt:
                    params = {
                        "clOrdID": f"{order.client_order_id}_{str(uuid4())[:4]}"
                    }
                    logger.info({
                        "event": "BitmexManager.place_order",
                        "order": order,
                        "params": params,
                        "timestamp": datetime.now()
                    })
                    result = await order_func(symbol, side, quantity, price, params)
                    if result.get("status", None) is not None:
                        return result
                    raise Exception('{}')

        price = order.price or ticker.get("last_price_protected")
        side = BitmexOrder.convert_order_side(order.side)
        order_type = BitmexOrder.convert_order_type(order.order_type)
        quantity = BitmexOrder.calculate_order_quantity(margin, order.percent, price, order.leverage,
                                                        ticker)
        symbol = client.safe_symbol(order.symbol)

        order_func: typing.Callable = {
            OrderType.LIMIT: client.create_limit_order,
            OrderType.STOP: client.create_limit_order,
            OrderType.MARKET: client.create_market_order,
        }[order.order_type]

        return await execute_order()

    @staticmethod
    async def place_stop_order(client: ccxtpro.bitmex, stop_order: BitmexOrder, quantity, ticker):
        async def execute_stop_order():
            async for attempt in AsyncRetrying(reraise=True,
                                               stop=stop_after_attempt(3),
                                               retry=retry_unless_exception_type(FATAL_ORDER_EXCEPTIONS),
                                               wait=wait_random(min=5, max=20)):
                with attempt:
                    params: typing.Dict[str, typing.Any] = {
                        "stopPx": stop_price,
                        "clOrdID": f"{stop_order.client_order_id}_{str(uuid4())[:4]}",
                        "execInst": f"ReduceOnly,{trigger_type}",
                    }
                    logger.info({
                        "event": "BitmexManager.place_stop_order",
                        "order": stop_order,
                        "params": params,
                        "timestamp": datetime.now(),
                    })
                    result = await client.create_order(market_symbol, order_type, side, amount=quantity,
                                                       price=stop_price,
                                                       params=params)
                    if result.get("status", None) is not None:
                        return result
                    raise Exception('{}')

        trigger_type: typing.Optional[str] = BitmexOrder.convert_trigger_type(
            StopTriggerType(stop_order.stop_trigger_type)
        )
        if not trigger_type:
            logger.error({
                "event": "BitmexManager.place_stop_order",
                "error": "Invalid Stop Trigger Type",
                "timestamp": datetime.now()
            })
            raise ValueError("Invalid Stop Trigger Type")

        if not stop_order.stop_price:
            logger.error({
                "event": "BitmexManager.place_stop_order",
                "error": "Stop Price Missing",
                "timestamp": datetime.now(),
            })
            raise ValueError("Stop Price Missing")

        symbol: BitmexSymbol = create_symbol(ticker)

        # Rounds price off to the correct tick_size and digits
        stop_price = float(round(stop_order.stop_price, symbol.fractional_digits))
        stop_price = stop_price - (stop_price % symbol.tick_size)

        side = BitmexOrder.convert_order_side(stop_order.side)

        order_type: typing.Optional[str] = BitmexOrder.convert_order_type(OrderType.STOP)
        market_symbol = client.safe_symbol(symbol.symbol)

        return await execute_stop_order()

    @staticmethod
    async def place_tsl_order(client: ccxtpro.bitmex, tsl_order: BitmexOrder, quantity, ticker):
        async def execute_tsl_order():
            async for attempt in AsyncRetrying(reraise=True,
                                               stop=stop_after_attempt(3),
                                               retry=retry_unless_exception_type(FATAL_ORDER_EXCEPTIONS),
                                               wait=wait_random(min=5, max=20)):
                with attempt:
                    params: typing.Dict[str, typing.Any] = {
                        "stopPx": stop_price,
                        "pegOffsetValue": peg_offset_value,
                        "pegPriceType": "TrailingStopPeg",
                        "clOrdID": f"{tsl_order.client_order_id}_{str(uuid4())[:4]}",
                        "execInst": f"ReduceOnly,{trigger_type}",
                    }
                    logger.info({
                        "event": "BitmexManager.place_tsl_order",
                        "order": tsl_order,
                        "params": params,
                    })
                    result = await client.create_order(market_symbol, order_type,
                                                       BitmexOrder.convert_order_side(tsl_order.side),
                                                       amount=quantity, price=stop_price, params=params)
                    if result.get("status", None) is not None:
                        return result
                    raise Exception('{}')

        trigger_type: typing.Optional[str] = BitmexOrder.convert_trigger_type(
            StopTriggerType(tsl_order.stop_trigger_type)
        )
        if not trigger_type:
            logger.error({
                "event": "BitmexManager.place_tsl_order",
                "error": "Invalid Stop Trigger Type",
            })
            raise ValueError("Invalid Stop Trigger Type")

        if not tsl_order.trailing_stop_percent:
            logger.error({
                "event": "BitmexManager.place_tsl_order",
                "error": "Trailing Stop Percent Missing",
            })
            raise ValueError("Trailing Stop Percent Missing")

        symbol: BitmexSymbol = create_symbol(ticker)

        main_order_side = OrderSide.BUY if tsl_order.side == OrderSide.SELL else OrderSide.SELL

        trailing_offset_factor = 1 - (tsl_order.trailing_stop_percent / 100) if main_order_side == OrderSide.BUY else \
            1 + (tsl_order.trailing_stop_percent / 100)

        if tsl_order.price:
            huge_starting_offset = 10000
            peg_offset = huge_starting_offset * (-1 if main_order_side == OrderSide.BUY else 1)
            stop_price = tsl_order.price * trailing_offset_factor
        else:
            use_last_price = tsl_order.stop_trigger_type == StopTriggerType.LAST_PRICE
            current_price = symbol.last_price_protected if use_last_price else symbol.mark_price
            peg_offset = -1 * current_price * (1 - trailing_offset_factor)
            stop_price = current_price * trailing_offset_factor

        # Rounds price off to the correct tick_size and digits
        stop_price = float(round(stop_price, symbol.fractional_digits))
        stop_price = stop_price - (stop_price % symbol.tick_size)

        peg_offset_value = float(round(peg_offset, symbol.fractional_digits))
        peg_offset_value = peg_offset_value - (peg_offset_value % symbol.tick_size)

        order_type: typing.Optional[str] = BitmexOrder.convert_order_type(OrderType.STOP)
        market_symbol = client.safe_symbol(symbol.symbol)

        return await execute_tsl_order()

    @staticmethod
    async def close_position(
        client: ccxtpro.bitmex,
        order: BitmexOrder,
        position: BitmexPosition,
        ticker,
    ):
        async def execute_close():
            async for attempt in AsyncRetrying(reraise=True,
                                               stop=stop_after_attempt(3),
                                               retry=retry_unless_exception_type(FATAL_ORDER_EXCEPTIONS),
                                               wait=wait_random(min=5, max=20)):
                with attempt:
                    params: typing.Dict[str, typing.Any] = {
                        "execInst": "Close",
                        "clOrdID": f"{order.client_order_id}_{str(uuid4())[:4]}",
                    }

                    logger.info({
                        "event": "BitmexManager.close_position",
                        "order_type": order_type,
                        "side": side,
                        "symbol": symbol,
                        "order_quantity": order_quantity,
                        "price": price,
                        "params": params,
                        "timestamp": datetime.now(),
                    })
                    result = await client.create_order(symbol, order_type, side, order_quantity, price,
                                                       params=params)
                    if result.get("status", None) is not None:
                        return result
                    raise Exception('{}')

        symbol = client.safe_symbol(order.symbol)
        price = float(order.price or ticker.get("last_price_protected"))

        min_max_func = max if position.current_quantity > 0 else min
        order_quantity = -1 * min_max_func(1, round(order.percent * position.current_quantity)) / 100

        side = BitmexOrder.convert_order_side(order.side)

        order_type = BitmexOrder.convert_order_type(OrderType.LIMIT if price else OrderType.MARKET)

        return await execute_close()

    @staticmethod
    async def add_stop_to_position(
        client: ccxtpro.bitmex,
        symbol: BitmexSymbol,
        position: BitmexPosition,
        raw_price: float,
        trigger_price_type: StopTriggerType
    ):
        async def execute_order():
            async for attempt in AsyncRetrying(reraise=True,
                                               stop=stop_after_attempt(3),
                                               retry=retry_unless_exception_type(FATAL_ORDER_EXCEPTIONS),
                                               wait=wait_random(min=5, max=20)):
                with attempt:
                    params: typing.Dict[str, typing.Any] = {
                        "execInst": f"Close,{trigger_type}"
                    }

                    logger.info({
                        "event": "BitmexManager.add_stop_to_position",
                        "order_type": order_type,
                        "symbol": symbol,
                        "side": side,
                        "price": stop_price,
                        "params": params,
                        "timestamp": datetime.now(),
                    })
                    result = await client.create_order(market_symbol, order_type, side, amount=None, price=stop_price,
                                                       params=params)
                    if result.get("status", None) is not None:
                        return result
                    raise Exception('{}')

        trigger_type: typing.Optional[str] = BitmexOrder.convert_trigger_type(StopTriggerType(trigger_price_type))
        if not trigger_type:
            logger.error({
                "event": "BitmexManager.add_stop_to_position",
                "error": "Invalid Stop Trigger Type",
                "timestamp": datetime.now(),
            })
            raise ValueError("Invalid Stop Trigger Type")

        # Rounds price off to the correct tick_size and digits
        stop_price = float(round(raw_price, symbol.fractional_digits))
        stop_price = stop_price - (stop_price % symbol.tick_size)

        side = BitmexOrder.convert_order_side(position.side)

        order_type: typing.Optional[str] = BitmexOrder.convert_order_type(OrderType.STOP)
        market_symbol = client.safe_symbol(symbol.symbol)

        return await execute_order()

    @staticmethod
    async def add_tsl_to_position(
        client: ccxtpro.bitmex,
        symbol: BitmexSymbol,
        position: BitmexPosition,
        tsl_percent: float,
        trigger_price_type: StopTriggerType
    ):
        trigger_type: typing.Optional[str] = BitmexOrder.convert_trigger_type(StopTriggerType(trigger_price_type))
        if not trigger_type:
            logger.info({
                "event": "BitmexManager.add_tsl_to_position",
                "error": "Invalid Stop Trigger Type",
                "timestamp": datetime.now(),
            })
            raise ValueError("Invalid Stop Trigger Type")

        tsl_fraction = tsl_percent / 100
        trailing_offset_factor = (1 - tsl_fraction) if position.side == OrderSide.BUY else (1 + tsl_fraction)

        order_type: typing.Optional[str] = BitmexOrder.convert_order_type(OrderType.STOP)
        market_symbol = client.safe_symbol(symbol.symbol)

        use_last_price = trigger_price_type == StopTriggerType.LAST_PRICE.value
        current_price = symbol.last_price_protected if use_last_price else symbol.mark_price

        peg_offset = -1 * current_price * (1 - trailing_offset_factor)
        stop_price = current_price * trailing_offset_factor

        tsl_side = BitmexOrder.convert_order_side(OrderSide.SELL if position.side == OrderSide.BUY else OrderSide.BUY)
        stop_px = float(round(stop_price, symbol.fractional_digits))
        stop_px = stop_px - (stop_px % symbol.tick_size)

        peg_offset_value = float(round(peg_offset, symbol.fractional_digits))
        peg_offset_value = peg_offset_value - (peg_offset_value % symbol.tick_size)

        params: typing.Dict[str, typing.Any] = {
            "stopPx": stop_px,
            "pegPriceType": "TrailingStopPeg",
            "pegOffsetValue": peg_offset_value,
            "execInst": f"Close,{trigger_type}",
        }

        logger.info({
            "event": "BitmexManager.add_tsl_to_position",
            "symbol": market_symbol,
            "order_type": order_type,
            "side": tsl_side,
            "price": stop_price,
            "params": params,
            "timestamp": datetime.now(),
        })

        return await client.create_order(market_symbol, order_type, tsl_side, amount=None, price=stop_price,
                                         params=params)

    @staticmethod
    async def cancel_order(client: ccxtpro.bitmex, order_id: str):
        async for attempt in AsyncRetrying(reraise=True,
                                           stop=stop_after_attempt(3),
                                           retry=retry_unless_exception_type(FATAL_ORDER_EXCEPTIONS),
                                           wait=wait_random(min=5, max=20)):
            with attempt:
                logger.info({
                    "event": "BitmexManager.cancel_order",
                    "order_id": order_id,
                    "timestamp": datetime.now(),
                })
                return await client.cancel_order(order_id)

    @staticmethod
    async def set_position_leverage(client: ccxtpro.bitmex, symbol: str, leverage: float):
        async for attempt in AsyncRetrying(reraise=True,
                                           stop=stop_after_attempt(3),
                                           retry=retry_unless_exception_type(FATAL_ORDER_EXCEPTIONS),
                                           wait=wait_random(min=5, max=20)):
            with attempt:
                logger.info({
                    "event": "BitmexManager.set_position_leverage",
                    "symbol": symbol,
                    "leverage": leverage,
                    "timestamp": datetime.now(),
                })
                result = await client.privatePostPositionLeverage({"symbol": symbol, "leverage": leverage})
                if result.get("leverage", None) is not None:
                    return result
                raise Exception('{}')

    async def watch_my_trades_stream(self, client_id: str, client: ccxtpro.bitmex):
        while self._watching_streams:
            try:
                await client.watch_my_trades()
                await self.update_my_trades_data(client_id, client.myTrades)
            except Exception:
                pass

    async def watch_positions_stream(self, client_id: str, client: ccxtpro.bitmex):
        while self._watching_streams:
            try:
                await client.watch_positions()
                await self.update_positions_data(client_id, client.positions)
            except Exception:
                pass

    async def watch_tickers_stream(self, client_id: str, client: ccxtpro.bitmex):
        while self._watching_streams:
            try:
                await client.watch_instruments()
                await self.update_ticker_data(client_id, client.tickers)
            except Exception:
                pass

    async def watch_balance_stream(self, client_id: str, client: ccxtpro.bitmex):
        while self._watching_streams:
            try:
                await client.watch_balance()
                await self.update_margin_data(client_id, client.balance)
            except Exception:
                pass

    async def watch_orders_stream(self, client_id: str, client: ccxtpro.bitmex):
        while self._watching_streams:
            try:
                await client.watch_orders()
                await self.update_orders_data(client_id, client.orders)
            except Exception:
                pass

    async def update_ticker_data(self, client_id: str, data: typing.Dict):
        if not data:
            return

        tickers: typing.Dict = {}
        for ticker in data.values():
            info = ticker.get("info")
            if not info.get("state") in ("Open",):
                continue
            symbol = info.get("symbol")
            tickers[symbol] = info
        await self.emit_ticker_updated_event(client_id, tickers)

    async def update_margin_data(self, client_id: str, data: typing.Dict):
        if not data:
            return

        await self.emit_margins_updated_event(client_id, data)

    async def update_orders_data(self, client_id: str, data: typing.Dict):
        if not data:
            return

        # ccxt updates give us data for ALL orders even if they were not part of the update.
        # this code will filter out the stuff that didn't change.
        for order in data:
            order_id = order["id"]
            order_hash = hash(json.dumps(order))

            if self._orders_cache.get(order_id, None) == order_hash:
                continue

            self._orders_cache[order["id"]] = order_hash
            await self.emit_order_updated_event(order)

    async def update_positions_data(self, client_id: str, data: typing.Dict):
        if not data:
            return

        # ccxt updates give us data for ALL positions even if they were not part of the update.
        # this code will filter out the stuff that didn't change.
        updated_positions = []
        for position in data.values():
            position_symbol = position["symbol"]
            position_hash = hash(json.dumps(position))

            if self._positions_cache.get(position_symbol, None) == position_hash:
                continue

            updated_positions.append(position)
            self._positions_cache[position_symbol] = position_hash

        await self.emit_positions_updated_event(client_id, updated_positions)

    async def update_my_trades_data(self, client_id: str, data: typing.Dict):
        if not data:
            return

        await self.emit_my_trades_updated_event(client_id, data)


def _is_successful_order(value):
    return value.get("status", None) is not None


bitmex_manager = BitmexManager(event_bus)
