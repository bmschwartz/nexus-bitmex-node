import json

import glom
from attr import dataclass
from glom import Coalesce

POSITION_SPEC = {
    "symbol": "symbol",
    "is_open": Coalesce("isOpen", default=False),
    "currency": Coalesce("currency", default=None),
    "underlying": Coalesce("underlying", default=None),
    "quote_currency": Coalesce("quoteCurrency", default=None),
    "leverage": Coalesce("leverage", default=None),
    "simple_quantity": Coalesce("simpleQty", default=0),
    "current_quantity": Coalesce("currentQty", default=0),
    "mark_price": Coalesce("markPrice", default=None),
    "margin": Coalesce("posMargin", default=None),
    "maintenance_margin": Coalesce("maintMargin", default=None),
}

POSITION_JSON_SPEC = {
    "symbol": "symbol",
    "is_open": "is_open",
    "currency": "currency",
    "underlying": "underlying",
    "quote_currency": "quote_currency",
    "leverage": "leverage",
    "simple_quantity": "simple_quantity",
    "current_quantity": "current_quantity",
    "mark_price": "mark_price",
    "margin": "margin",
    "maintenance_margin": "maintenance_margin"
}


@dataclass
class BitmexPosition:
    symbol: str
    is_open: bool
    currency: str
    underlying: str
    quote_currency: str
    leverage: float
    simple_quantity: float
    current_quantity: float
    mark_price: float
    margin: float
    maintenance_margin: float

    def to_json(self):
        return json.dumps({
            "symbol": self.symbol,
            "is_open": self.is_open,
            "currency": self.currency,
            "underlying": self.underlying,
            "quote_currency": self.quote_currency,
            "leverage": self.leverage,
            "simple_quantity": self.simple_quantity,
            "current_quantity": self.current_quantity,
            "mark_price": self.mark_price,
            "margin": self.margin,
            "maintenance_margin": self.maintenance_margin
        })


def create_position(position_data: dict) -> BitmexPosition:
    try:
        glommed = glom.glom(position_data, POSITION_SPEC)
    except (glom.core.PathAccessError, KeyError):
        glommed = glom.glom(position_data, POSITION_JSON_SPEC)
    return BitmexPosition(**glommed)
