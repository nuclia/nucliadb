from typing import Optional

from nucliadb_protos.resources_pb2 import Basic

from nucliadb_ingest.maindb.driver import Transaction
from nucliadb_ingest.orm.local_node import LocalNode
from nucliadb_ingest.orm.node import Node
from nucliadb_ingest.settings import settings as ingest_settings
from nucliadb_utils.settings import indexing_settings

KB_RESOURCE_BASIC_FS = "/kbs/{kbid}/r/{uuid}/basic"  # Only used on FS driver
KB_RESOURCE_BASIC = "/kbs/{kbid}/r/{uuid}"


def get_node_klass():
    if indexing_settings.index_local:
        return LocalNode
    else:
        return Node


async def set_basic(txn: Transaction, kbid: str, uuid: str, basic: Basic):
    if ingest_settings.driver == "local":
        await txn.set(
            KB_RESOURCE_BASIC_FS.format(kbid=kbid, uuid=uuid),
            basic.SerializeToString(),
        )
    else:
        await txn.set(
            KB_RESOURCE_BASIC.format(kbid=kbid, uuid=uuid),
            basic.SerializeToString(),
        )


async def get_basic(txn: Transaction, kbid: str, uuid: str) -> Optional[bytes]:
    if ingest_settings.driver == "local":
        raw_basic = await txn.get(KB_RESOURCE_BASIC_FS.format(kbid=kbid, uuid=uuid))
    else:
        raw_basic = await txn.get(KB_RESOURCE_BASIC.format(kbid=kbid, uuid=uuid))
    return raw_basic
