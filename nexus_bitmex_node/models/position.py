import enum
import json

import glom
from attr import dataclass
from glom import Coalesce

from nexus_bitmex_node.models.base import BitmexBaseModel
from nexus_bitmex_node.models.order import OrderSide


class PositionSide(enum.Enum):
    LONG = "Long"
    SHORT = "Short"


POSITION_SPEC = {
    "symbol": "symbol",
    "is_open": Coalesce("isOpen", default=None),
    "currency": Coalesce("currency", default=None),
    "underlying": Coalesce("underlying", default=None),
    "quote_currency": Coalesce("quoteCurrency", default=None),
    "leverage": Coalesce("leverage", default=None),
    "simple_quantity": Coalesce("simpleQty", default=None),
    "current_quantity": Coalesce("currentQty", default=None),
    "mark_price": Coalesce("markPrice", default=None),
    "margin": Coalesce("posMargin", default=None),
    "maintenance_margin": Coalesce("maintMargin", default=None),
    "average_entry_price": Coalesce("avgEntryPrice", default=None),
}


@dataclass
class BitmexPosition(BitmexBaseModel):
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
    average_entry_price: float

    def to_json(self):
        return json.dumps({
            "symbol": self.symbol,
            "is_open": self.current_quantity not in (0, None),
            "currency": self.currency,
            "underlying": self.underlying,
            "quote_currency": self.quote_currency,
            "leverage": self.leverage,
            "simple_quantity": self.simple_quantity,
            "current_quantity": self.current_quantity,
            "mark_price": self.mark_price,
            "margin": self.margin,
            "maintenance_margin": self.maintenance_margin,
            "average_entry_price": self.average_entry_price,
        })

    @property
    def side(self):
        return OrderSide.BUY if self.current_quantity > 0 else OrderSide.SELL


def create_position(position_data: dict, local=False) -> BitmexPosition:
    if local:
        return BitmexPosition(**position_data)

    try:
        glommed = glom.glom(position_data, POSITION_SPEC)
        return BitmexPosition(**glommed)
    except (glom.core.CoalesceError, glom.core.PathAccessError, KeyError):
        return BitmexPosition(**position_data)
