import enum
import typing

import glom
import ccxtpro
from attr import dataclass


class OrderSide(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(enum.Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP = "STOP"


class StopTriggerType(enum.Enum):
    LAST_PRICE = "LAST_PRICE"
    MARK_PRICE = "MARK_PRICE"
    NONE = None


ORDER_SPEC = {
    "id": ("orderId", str),
    "symbol": "symbol",
    "side": ("side", OrderSide),
    "order_type": ("orderType", OrderType),
    "price": "price",
    "stop_price": "stopPrice",
    "percent": "percent",
    "leverage": "leverage",
    "stop_trigger_type": ("stopTriggerType", StopTriggerType),
    "trailing_stop_percent": "trailingStopPercent",
}

mXBT_TO_XBT_FACTOR = 1 / 1000
XBt_TO_XBT_FACTOR = 1 / 100000000

CONTRACT_VALUE_MULTIPLIERS = {
    "ETHUSD": 0.001 * mXBT_TO_XBT_FACTOR
}


@dataclass
class BitmexOrder:
    id: int
    symbol: str
    side: OrderSide
    order_type: OrderType
    percent: float
    leverage: float
    price: typing.Optional[float]
    stop_price: typing.Optional[float]
    stop_trigger_type: typing.Optional[StopTriggerType]
    trailing_stop_percent: typing.Optional[float]

    async def place_order(self, client: ccxtpro.bitmex, ticker, margin, position):
        price = self.price or ticker.get("lastPrice")
        side = BitmexOrder.convert_order_side(self.side)
        order_type = BitmexOrder.convert_order_type(self.order_type)
        quantity = await calculate_order_quantity(margin, self.percent, price, self.leverage, ticker)

        symbol = client.safe_symbol(self.symbol)

        return await client.create_limit_order(symbol, side, quantity, price)

    @staticmethod
    def convert_order_type(order_type: OrderType) -> str:
        if order_type == OrderType.LIMIT:
            return "Limit"
        elif order_type == OrderType.MARKET:
            return "Market"
        elif order_type == OrderType.STOP:
            return "Stop"
        return ""

    @staticmethod
    def convert_order_side(side: OrderSide) -> str:
        if side == OrderSide.BUY:
            return "Buy"
        elif side == OrderSide.SELL:
            return "Sell"
        return ""


def create_order(order_data: dict) -> BitmexOrder:
    glommed = glom.glom(order_data, ORDER_SPEC)
    return BitmexOrder(**glommed)


async def calculate_order_quantity(margin: float, percent: float, price: float, leverage: float, ticker: typing.Dict):
    if percent > 0:
        percent /= 100.0
    else:
        return 0

    margin_to_spend = round(percent * margin, 8)
    symbol_value = get_symbol_value_in_xbt(ticker, price)
    return round(margin_to_spend * leverage / symbol_value)


def get_symbol_value_in_xbt(ticker, price: float) -> float:
    if ticker.get("underlying") == "XBT":
        return 1 / price

    multiplier = CONTRACT_VALUE_MULTIPLIERS.get(ticker["symbol"], 1)
    return price * multiplier
