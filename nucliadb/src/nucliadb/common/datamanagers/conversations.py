"""MarkLogic datamanager for paginated conversation fields."""

import json
from typing import TypeVar
from urllib.parse import quote

from google.protobuf.message import Message

from nucliadb.common.datamanagers import fields
from nucliadb.common.datamanagers import marklogic_documents as documents
from nucliadb.common.datamanagers.marklogic_documents import database
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import FieldConversation, SplitsMetadata

_SPLITS_METADATA_PAGE = 0

PB = TypeVar("PB", bound=Message)


def _directory(rid: str, field_id: str) -> str:
    return f"/resources/{rid}/conversations/{quote(field_id, safe='')}/"


def _page_uri(rid: str, field_id: str, page: int) -> str:
    return f"{_directory(rid, field_id)}{page}.json"


async def _read(
    txn: Transaction, kbid: str, rid: str, field_id: str, page: int, pb_klass: type[PB]
) -> PB | None:
    content = await documents.read(txn, database(txn, kbid), _page_uri(rid, field_id, page))
    if content is None or content.get("value") is None:
        return None
    return documents.from_json(content["value"], pb_klass)


async def _write(
    txn: Transaction, kbid: str, rid: str, field_id: str, page: int, value: Message
) -> None:
    await documents.write(
        txn,
        database(txn, kbid),
        _page_uri(rid, field_id, page),
        MarkLogicCollections.CONVERSATIONS,
        {
            "rid": rid,
            "field_id": field_id,
            "page": page,
            "value": documents.to_json(value),
        },
    )


async def get_metadata(
    txn: Transaction, *, kbid: str, rid: str, field_id: str
) -> FieldConversation | None:
    return await fields.get(
        txn, kbid=kbid, rid=rid, field_type="c", field_id=field_id, pb_klass=FieldConversation
    )


async def set_metadata(
    txn: Transaction, *, kbid: str, rid: str, field_id: str, metadata: FieldConversation
) -> None:
    await fields.set(txn, kbid=kbid, rid=rid, field_type="c", field_id=field_id, value=metadata)


async def get_page(
    txn: Transaction, *, kbid: str, rid: str, field_id: str, page: int
) -> PBConversation | None:
    if page <= 0:
        raise ValueError("Conversation pages start at index 1")
    return await _read(txn, kbid, rid, field_id, page, PBConversation)


async def set_page(
    txn: Transaction, *, kbid: str, rid: str, field_id: str, page: int, value: PBConversation
) -> None:
    if page <= 0:
        raise ValueError("Conversation pages start at index 1")
    await _write(txn, kbid, rid, field_id, page, value)


async def get_splits_metadata(
    txn: Transaction, *, kbid: str, rid: str, field_id: str
) -> SplitsMetadata | None:
    return await _read(txn, kbid, rid, field_id, _SPLITS_METADATA_PAGE, SplitsMetadata)


async def set_splits_metadata(
    txn: Transaction, *, kbid: str, rid: str, field_id: str, splits_metadata: SplitsMetadata
) -> None:
    await _write(txn, kbid, rid, field_id, _SPLITS_METADATA_PAGE, splits_metadata)


async def delete_pages(txn: Transaction, *, kbid: str, rid: str, field_id: str) -> None:
    driver, marklogic_txn = documents.driver_txn(txn, ensure_writes=True)
    db = driver.kb_database(kbid)
    dsl = (
        "op.fromDocUris(cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.CONVERSATIONS)}), "
        f"cts.directoryQuery({json.dumps(_directory(rid, field_id))}, 'infinity')"
        "])).remove()"
    )
    response = await driver.client.rows.update(dsl=dsl, params=await marklogic_txn.params(db))
    driver.data._check(response, "delete conversation pages")


async def delete_field(txn: Transaction, *, kbid: str, rid: str, field_id: str) -> None:
    await fields.delete(txn, kbid=kbid, rid=rid, field_type="c", field_id=field_id)
