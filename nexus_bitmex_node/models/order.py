import enum
import typing

import glom
from attr import dataclass


class OrderSide(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(enum.Enum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


ORDER_SPEC = {
    "id": ("orderId", str),
    "symbol": "symbol",
    "side": ("side", OrderSide),
    "order_type": ("orderType", OrderType),
    "price": "price",
    "stop_price": "stopPrice",
    "percent": "percent",
}


@dataclass
class BitmexOrder:
    id: int
    symbol: str
    side: OrderSide
    order_type: OrderType
    price: typing.Optional[float]
    stop_price: typing.Optional[float]
    percent: float


def create_order(order_data: dict) -> BitmexOrder:
    glommed = glom.glom(order_data, ORDER_SPEC)
    return BitmexOrder(**glommed)


def calculate_order_quantity(margin: float, percent: float, price: float, leverage: float) -> float:
    # TODO: Implement this
    return 0.0
