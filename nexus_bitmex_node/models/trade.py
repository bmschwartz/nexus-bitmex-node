import enum
import json
import typing

import glom
from attr import dataclass
from glom import Coalesce

from nexus_bitmex_node.models.base import BitmexBaseModel


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
    "client_order_id": Coalesce("clOrdID", default=None),
    "client_order_link_id": Coalesce("clOrdLinkID", default=None),
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
class BitmexTrade(BitmexBaseModel):
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
        return BitmexTrade(**glommed)
    except (KeyError, TypeError):
        data = {
            "order_id": trade_data.get("orderID"),
            "symbol": trade_data.get("symbol"),
            "side": trade_data.get("side"),
            "order_quantity": trade_data.get("orderQty"),
            "order_type": trade_data.get("ordType"),
            "order_status": trade_data.get("ordStatus"),
            "filled_quantity": trade_data.get("cumQty"),
            "avg_price": trade_data.get("avgPx"),
            "client_order_id": trade_data.get("clOrdID"),
            "client_order_link_id": trade_data.get("clOrdLinkID"),
            "peg_price_type": trade_data.get("pegPriceType"),
            "peg_offset_value": trade_data.get("pegOffsetValue"),
            "text": trade_data.get("text"),
            "stop_price": trade_data.get("stopPx")
        }
        return BitmexTrade(**data)
