import glom
from attr import dataclass


POSITION_SPEC = {
    "symbol": "symbol",
    "is_open": "isOpen",
    "currency": "currency",
    "underlying": "underlying",
    "quote_currency": "quoteCurrency",
    "leverage": "leverage",
    "simple_quantity": "simpleQty",
    "current_quantity": "currentQty",
    "mark_price": "markPrice",
    "margin": "posMargin",
    "maintenance_margin": "maintMargin",
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
        return {
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


def create_position(position_data: dict) -> BitmexPosition:
    try:
        glommed = glom.glom(position_data, POSITION_SPEC)
    except KeyError:
        glommed = glom.glom(position_data, POSITION_JSON_SPEC)
    return BitmexPosition(**glommed)
