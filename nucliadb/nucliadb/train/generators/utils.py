from contextvars import ContextVar
from typing import Dict, Optional

from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.utils import get_driver
from nucliadb.train import SERVICE_NAME
from nucliadb_utils.utilities import get_cache, get_storage

rcache: ContextVar[Optional[Dict[str, ResourceORM]]] = ContextVar(
    "rcache", default=None
)

txn: ContextVar[Optional[Transaction]] = ContextVar("txn", default=None)


def get_resource_cache(clear: bool = False) -> Dict[str, ResourceORM]:
    value: Optional[Dict[str, ResourceORM]] = rcache.get()
    if value is None or clear:
        value = {}
        rcache.set(value)
    return value


async def get_transaction() -> Transaction:
    transaction: Optional[Transaction] = txn.get()
    if transaction is None:
        driver = await get_driver()
        transaction = await driver.begin()
        txn.set(transaction)
    return transaction


async def get_resource_from_cache(kbid: str, uuid: str) -> Optional[ResourceORM]:
    resouce_cache = get_resource_cache()
    orm_resource: Optional[ResourceORM] = None
    if uuid not in resouce_cache:
        transaction = await get_transaction()
        storage = await get_storage(service_name=SERVICE_NAME)
        cache = await get_cache()
        kb = KnowledgeBoxORM(transaction, storage, cache, kbid)
        orm_resource = await kb.get(uuid)
        if orm_resource is not None:
            resouce_cache[uuid] = orm_resource
    else:
        orm_resource = resouce_cache.get(uuid)
    return orm_resource
