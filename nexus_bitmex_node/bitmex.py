import typing

import ccxtpro

from nexus_bitmex_node.event_bus import (
    EventBus,
    event_bus,
    ExchangeEventEmitter,
)
from nexus_bitmex_node.models.order import BitmexOrder, OrderSide, OrderType, StopTriggerType
from nexus_bitmex_node.models.position import BitmexPosition
from nexus_bitmex_node.models.symbol import BitmexSymbol


class BitmexManager(ExchangeEventEmitter):
    _symbol_data: dict
    _client: ccxtpro.bitmex
    _client_id: str
    _watching_streams: bool

    def __init__(self, bus: EventBus):
        ExchangeEventEmitter.__init__(self, bus)

        self._symbol_data = {}

    def stop_streams(self):
        self._watching_streams = False

    @staticmethod
    async def place_order(client: ccxtpro.bitmex, order: BitmexOrder, ticker, margin):
        price = order.price or ticker.get("last_price_protected")
        side = BitmexOrder.convert_order_side(order.side)
        order_type = BitmexOrder.convert_order_type(order.order_type)
        quantity = await BitmexOrder.calculate_order_quantity(margin, order.percent, price, order.leverage, ticker)
        symbol = client.safe_symbol(order.symbol)

        order_func = {
            OrderType.LIMIT: client.create_limit_order,
            OrderType.STOP: client.create_limit_order,
            OrderType.MARKET: client.create_market_order,
        }[order.order_type]

        return await order_func(symbol, side, quantity, price)

    @staticmethod
    async def close_position(
        client: ccxtpro.bitmex,
        symbol: str,
        position: BitmexPosition,
        price: typing.Optional[float] = None,
        fraction: typing.Optional[float] = None,
    ):
        symbol = client.safe_symbol(symbol)
        params: typing.Dict[str, typing.Any] = {"execInst": "Close"}
        order_quantity = None

        if fraction:
            min_max_func = max if position.current_quantity > 0 else min
            order_quantity = -1 * min_max_func(1, round(fraction * position.current_quantity))

        side = BitmexOrder.convert_order_side(position.side)

        order_type = BitmexOrder.convert_order_type(OrderType.LIMIT if price else OrderType.MARKET)
        return await client.create_order(symbol, order_type, side, order_quantity, price, params=params)

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
        while True:
            await client.watch_my_trades()
            await self.update_my_trades_data(client_id, client.myTrades)

    async def watch_positions_stream(self, client_id: str, client: ccxtpro.bitmex):
        while True:
            await client.watch_positions()
            await self.update_positions_data(client_id, client.positions)

    async def watch_tickers_stream(self, client_id: str, client: ccxtpro.bitmex):
        while True:
            await client.watch_instruments()
            await self.update_ticker_data(client_id, client.tickers)

    async def watch_balance_stream(self, client_id: str, client: ccxtpro.bitmex):
        while True:
            await client.watch_balance()
            await self.update_margin_data(client_id, client.balance)

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

    async def update_positions_data(self, client_id: str, data: typing.Dict):
        if not data:
            return

        await self.emit_positions_updated_event(client_id, list(data.values()))

    async def update_my_trades_data(self, client_id: str, data: typing.Dict):
        if not data:
            return

        await self.emit_my_trades_updated_event(client_id, data)


bitmex_manager = BitmexManager(event_bus)
