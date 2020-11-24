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


def create_position(position_data: dict) -> BitmexPosition:
    glommed = glom.glom(position_data, POSITION_SPEC)
    return BitmexPosition(**glommed)
