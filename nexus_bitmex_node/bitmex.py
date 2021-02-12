import json
import typing
from uuid import uuid4

import ccxtpro
from tenacity import retry, wait_exponential, stop_after_attempt

from nexus_bitmex_node.event_bus import (
    EventBus,
    event_bus,
    ExchangeEventEmitter, OrderEventEmitter,
)
from nexus_bitmex_node.models.order import BitmexOrder, OrderSide, OrderType, StopTriggerType
from nexus_bitmex_node.models.position import BitmexPosition
from nexus_bitmex_node.models.symbol import BitmexSymbol, create_symbol


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
        @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=12))
        async def execute_order():
            params = {
                "clOrdID": f"{order.client_order_id}_{str(uuid4())[:4]}"
            }
            return await order_func(symbol, side, quantity, price, params)

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

        order_result = await execute_order()

        return order_result

    @staticmethod
    async def place_stop_order(client: ccxtpro.bitmex, stop_order: BitmexOrder, quantity, ticker):
        @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=12))
        async def execute_stop_order():
            try:
                params: typing.Dict[str, typing.Any] = {
                    "stopPx": stop_price,
                    "clOrdID": f"{stop_order.client_order_id}_{str(uuid4())[:4]}",
                    "execInst": f"ReduceOnly,{trigger_type}",
                }
                return await client.create_order(market_symbol, order_type, side, amount=quantity, price=stop_price,
                                                 params=params)
            except Exception as e:
                print(e)
                return None

        trigger_type: typing.Optional[str] = BitmexOrder.convert_trigger_type(
            StopTriggerType(stop_order.stop_trigger_type)
        )
        if not trigger_type:
            raise ValueError("Invalid Stop Trigger Type")

        if not stop_order.stop_price:
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
        @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=12))
        async def execute_tsl_order():
            try:
                params: typing.Dict[str, typing.Any] = {
                    "stopPx": stop_price,
                    "pegOffsetValue": peg_offset_value,
                    "pegPriceType": "TrailingStopPeg",
                    "clOrdID": f"{tsl_order.client_order_id}_{str(uuid4())[:4]}",
                    "execInst": f"ReduceOnly,{trigger_type}",
                }
                return await client.create_order(market_symbol, order_type,
                                                 BitmexOrder.convert_order_side(tsl_order.side),
                                                 amount=quantity, price=stop_price, params=params)
            except Exception as e:
                print(e)
                return None

        trigger_type: typing.Optional[str] = BitmexOrder.convert_trigger_type(
            StopTriggerType(tsl_order.stop_trigger_type)
        )
        if not trigger_type:
            raise ValueError("Invalid Stop Trigger Type")

        if not tsl_order.trailing_stop_percent:
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
    ):
        symbol = client.safe_symbol(order.symbol)
        params: typing.Dict[str, typing.Any] = {
            "execInst": "Close",
            "clOrdID": f"{order.client_order_id}_{str(uuid4())[:4]}",
        }

        min_max_func = max if position.current_quantity > 0 else min
        order_quantity = -1 * min_max_func(1, round(order.percent * position.current_quantity)) / 100

        side = BitmexOrder.convert_order_side(order.side)

        order_type = BitmexOrder.convert_order_type(OrderType.LIMIT if order.price else OrderType.MARKET)
        return await client.create_order(symbol, order_type, side, order_quantity, order.price, params=params)

    @staticmethod
    async def add_stop_to_position(
        client: ccxtpro.bitmex,
        symbol: BitmexSymbol,
        position: BitmexPosition,
        raw_price: float,
        trigger_price_type: StopTriggerType
    ):
        trigger_type: typing.Optional[str] = BitmexOrder.convert_trigger_type(StopTriggerType(trigger_price_type))
        if not trigger_type:
            raise ValueError("Invalid Stop Trigger Type")
        params: typing.Dict[str, typing.Any] = {"execInst": f"Close,{trigger_type}"}

        # Rounds price off to the correct tick_size and digits
        stop_price = float(round(raw_price, symbol.fractional_digits))
        stop_price = stop_price - (stop_price % symbol.tick_size)

        side = BitmexOrder.convert_order_side(position.side)

        order_type: typing.Optional[str] = BitmexOrder.convert_order_type(OrderType.STOP)
        market_symbol = client.safe_symbol(symbol.symbol)

        return await client.create_order(market_symbol, order_type, side, amount=None, price=stop_price, params=params)

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

        return await client.create_order(market_symbol, order_type, tsl_side, amount=None, price=stop_price, params=params)

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


bitmex_manager = BitmexManager(event_bus)
