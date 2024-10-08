import enum
import json
import math
import typing
import glom
from attr import dataclass
from glom import Coalesce

from nexus_bitmex_node.models.base import BitmexBaseModel


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
    "id": (Coalesce("id", "orderId"), str),
    "client_order_id": ("clOrderId", str),
    "symbol": "symbol",
    "side": ("side", OrderSide),
    "order_type": ("orderType", OrderType),
    "close_order": ("closeOrder", bool),
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
    "ADAUSDTH21": 0.01,
    "BCHUSD": 0.001 * mXBT_TO_XBT_FACTOR,
    "BNBUSDTH21": 0.0001,
    "DOGEUSDT": 0.001,
    "DOTUSDTH21": 0.0001,
    "EOSUSDTH21": 0.0001,
    "ETHUSD": 0.001 * mXBT_TO_XBT_FACTOR,
    "ETHUSDH21": 0.001 * mXBT_TO_XBT_FACTOR,
    "LINKUSDT": 0.0001,
    "LINKUSDTH21": 0.0001,
    "LTCUSD": 0.002 * mXBT_TO_XBT_FACTOR,
    "XRPUSD": 0.0002,
    "XTZUSDTH21": 0.0001,
    "YFIUSDTH21": 0.0001 * mXBT_TO_XBT_FACTOR,
}


@dataclass
class BitmexOrder(BitmexBaseModel):
    id: int
    client_order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    close_order: bool
    percent: float
    leverage: float
    price: typing.Optional[float]
    stop_price: typing.Optional[float]
    stop_trigger_type: typing.Optional[StopTriggerType]
    trailing_stop_percent: typing.Optional[float]

    @staticmethod
    def convert_order_type(order_type: OrderType) -> typing.Optional[str]:
        if order_type == OrderType.LIMIT:
            return "Limit"
        elif order_type == OrderType.MARKET:
            return "Market"
        elif order_type == OrderType.STOP:
            return "Stop"
        return None

    @staticmethod
    def convert_order_side(side: OrderSide) -> str:
        if side == OrderSide.BUY:
            return "Buy"
        elif side == OrderSide.SELL:
            return "Sell"
        return ""

    @staticmethod
    def convert_trigger_type(trigger_type: StopTriggerType) -> typing.Optional[str]:
        if trigger_type == StopTriggerType.LAST_PRICE:
            return "LastPrice"
        elif trigger_type == StopTriggerType.MARK_PRICE:
            return "MarkPrice"
        return None

    @staticmethod
    def calculate_order_quantity(margin: float, percent: float, price: float, leverage: float, ticker: typing.Dict):
        if percent > 0:
            percent /= 100.0
        else:
            return 0

        margin_to_spend = round(percent * margin, 8)
        symbol_value = BitmexOrder.get_symbol_value_in_xbt(ticker, price)
        return math.floor(margin_to_spend * leverage / symbol_value)

    @staticmethod
    def get_symbol_value_in_xbt(ticker, price: float) -> float:
        if ticker.get("underlying") == "XBT":
            return 1 / price

        multiplier = CONTRACT_VALUE_MULTIPLIERS.get(ticker["symbol"], 1)
        return price * multiplier

    def to_json(self):
        return json.dumps({
            "id": self.id,
            "client_order_id": self.client_order_id,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "close_order": self.close_order,
            "percent": self.percent,
            "leverage": self.leverage,
            "price": self.price,
            "stop_price": self.stop_price,
            "stop_trigger_type": self.stop_trigger_type,
            "trailing_stop_percent": self.trailing_stop_percent,
        })


def create_order(order_data: dict) -> BitmexOrder:
    try:
        glommed = glom.glom(order_data, ORDER_SPEC)
        return BitmexOrder(**glommed)
    except (glom.core.PathAccessError, KeyError):
        return BitmexOrder(**order_data)
