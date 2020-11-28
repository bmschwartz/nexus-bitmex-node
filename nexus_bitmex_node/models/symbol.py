import json

import glom
from attr import dataclass

from nexus_bitmex_node.models.base import BitmexBaseModel

SYMBOL_SPEC = {
    "symbol": "symbol",
    "state": "state",
    "currency": "settlCurrency",
    "underlying": "underlying",
    "quote_currency": "quoteCurrency",
    "mark_price": "markPrice",
    "lot_size": "lotSize",
    "max_price": "maxPrice",
    "max_order_qty": "maxOrderQty",
    "tick_size": "tickSize",
    "last_price_protected": "lastPriceProtected"
}


@dataclass
class BitmexSymbol(BitmexBaseModel):
    symbol: str
    state: str
    currency: str
    underlying: str
    quote_currency: str
    mark_price: float
    lot_size: float
    max_price: float
    max_order_qty: float
    tick_size: float
    last_price_protected: float

    def to_json(self) -> str:
        return json.dumps({
            "symbol": self.symbol,
            "state": self.state,
            "currency": self.currency,
            "underlying": self.underlying,
            "quote_currency": self.quote_currency,
            "mark_price": self.mark_price,
            "lot_size": self.lot_size,
            "max_price": self.max_price,
            "max_order_qty": self.max_order_qty,
            "tick_size": self.tick_size,
            "last_price_protected": self.last_price_protected
        })

    @property
    def is_open(self):
        return self.state == 'Open'


def create_symbol(symbol_data: dict) -> BitmexSymbol:
    try:
        glommed = glom.glom(symbol_data, SYMBOL_SPEC)
        return BitmexSymbol(**glommed)
    except (glom.core.PathAccessError, KeyError):
        if "is_open" in symbol_data:
            symbol_data.pop("is_open")
        return BitmexSymbol(**symbol_data)
