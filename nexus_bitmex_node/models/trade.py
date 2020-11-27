import enum
import json
import typing

import glom
from attr import dataclass


class TradeOrderStatus(enum.Enum):
    NEW = "New"
    FILLED = "Filled"
    CANCELED = "Canceled"


class TradeOrderType(enum.Enum):
    LIMIT = "Limit"
    MARKET = "Market"
    STOP = "Stop"


class TradeOrderSide(enum.Enum):
    BUY = "Buy"
    SELL = "Sell"


TRADE_SPEC = {
    "order_id": "orderID",
    "symbol": "symbol",
    "side": "side",
    "order_type": "ordType",
    "order_status": "ordStatus",
    "order_quantity": ("orderQty", float),
    "filled_quantity": ("cumQty", float),
    "avg_price": "avgPx",
    "client_order_id": "clOrdID",
    "client_order_link_id": "clOrdLinkID",
    "peg_price_type": "pegPriceType",
    "peg_offset_value": "pegOffsetValue",
    "text": "text",
    "stop_price": "stopPx",
}

TRADE_JSON_SPEC = {
    "order_id": "order_id",
    "symbol": "symbol",
    "side": "side",
    "order_type": "order_type",
    "order_status": "order_status",
    "order_quantity": "order_quantity",
    "filled_quantity": "filled_quantity",
    "avg_price": "avg_price",
    "client_order_id": "client_order_id",
    "client_order_link_id": "client_order_link_id",
    "peg_price_type": "peg_price_type",
    "peg_offset_value": "peg_offset_value",
    "text": "text",
    "stop_price": "stop_price"
}


@dataclass
class BitmexTrade:
    order_id: str
    symbol: str
    side: TradeOrderSide
    order_type: TradeOrderType
    order_status: TradeOrderStatus
    order_quantity: float
    filled_quantity: float
    avg_price: typing.Optional[float]
    client_order_id: str
    client_order_link_id: str
    peg_price_type: typing.Optional[str]
    peg_offset_value: typing.Optional[str]
    text: str
    stop_price: typing.Optional[float]

    def to_json(self):
        return json.dumps({
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "order_status": self.order_status,
            "order_quantity": self.order_quantity,
            "filled_quantity": self.filled_quantity,
            "avg_price": self.avg_price,
            "client_order_id": self.client_order_id,
            "client_order_link_id": self.client_order_link_id,
            "peg_price_type": self.peg_price_type,
            "peg_offset_value": self.peg_offset_value,
            "text": self.text,
            "stop_price": self.stop_price,
        })


def create_trade(trade_data: dict) -> BitmexTrade:
    try:
        glommed = glom.glom(trade_data, TRADE_SPEC)
    except KeyError:
        glommed = glom.glom(trade_data, TRADE_JSON_SPEC)
    return BitmexTrade(**glommed)
