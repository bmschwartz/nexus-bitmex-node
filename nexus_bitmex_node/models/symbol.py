import glom
from attr import dataclass


SYMBOL_SPEC = {
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
class BitmexSymbol:
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


def create_symbol(symbol_data: dict) -> BitmexSymbol:
    glommed = glom.glom(symbol_data, SYMBOL_SPEC)
    return BitmexSymbol(**glommed)
