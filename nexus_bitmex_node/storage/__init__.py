from nexus_bitmex_node.event_bus import (
    event_bus,
    EventBus,
)
from nexus_bitmex_node.storage.data_store import DataStore
from nexus_bitmex_node.storage.local import LocalDataStore
from nexus_bitmex_node.storage.redis import RedisDataStore


def create_data_store(bus: EventBus) -> DataStore:
    return RedisDataStore(bus)


data_store = create_data_store(event_bus)
