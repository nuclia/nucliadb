"""MarkLogic datamanager for resource fields."""

import json
from collections.abc import Sequence
from typing import Any, TypeVar
from urllib.parse import quote

from google.protobuf.message import Message

from nucliadb.common.datamanagers import marklogic_documents as documents
from nucliadb.common.datamanagers.utils import observer
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.models_utils import from_proto, to_proto
from nucliadb_protos import resources_pb2 as rpb2
from nucliadb_protos import writer_pb2 as wpb2

PB = TypeVar("PB", bound=Message)


def _uri(rid: str, field_type: str, field_id: str) -> str:
    return f"/resources/{rid}/fields/{field_type}/{quote(field_id, safe='')}.json"


async def _database(txn: Transaction, kbid: str) -> str:
    driver, _ = documents.driver_txn(txn)
    return driver.kb_database(kbid)


async def _update(
    txn: Transaction, kbid: str, rid: str, field_type: str, field_id: str, **values: Any
) -> None:
    driver, _ = documents.driver_txn(txn)
    database = await driver.ensure_kb_database(kbid)
    uri = _uri(rid, field_type, field_id)
    content = await documents.read(txn, database, uri) or {
        "rid": rid,
        "field_type": field_type,
        "field_id": field_id,
    }
    content.update(values)
    await documents.write(txn, database, uri, MarkLogicCollections.FIELDS, content)


@observer.wrap({"type": "field", "op": "set_status"})
async def set_status(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str, status: wpb2.FieldStatus
) -> None:
    await _update(txn, kbid, rid, field_type, field_id, status=documents.to_json(status))


@observer.wrap({"type": "field", "op": "set"})
async def set(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str, value: Message
) -> None:
    await _update(txn, kbid, rid, field_type, field_id, value=documents.to_json(value))


@observer.wrap({"type": "field", "op": "delete"})
async def delete(txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str) -> None:
    if field_type == "c":
        from nucliadb.common.datamanagers import conversations

        await conversations.delete_pages(txn, kbid=kbid, rid=rid, field_id=field_id)
    await documents.delete(txn, await _database(txn, kbid), _uri(rid, field_type, field_id))


@observer.wrap({"type": "field", "op": "get"})
async def get(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str, pb_klass: type[PB]
) -> PB | None:
    content = await documents.read(txn, await _database(txn, kbid), _uri(rid, field_type, field_id))
    if content is None or content.get("value") is None:
        return None
    return documents.from_json(content["value"], pb_klass)


@observer.wrap({"type": "field", "op": "get_status"})
async def get_status(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str
) -> wpb2.FieldStatus | None:
    content = await documents.read(txn, await _database(txn, kbid), _uri(rid, field_type, field_id))
    if content is None or content.get("status") is None:
        return None
    return documents.from_json(content["status"], wpb2.FieldStatus)


@observer.wrap({"type": "field", "op": "get_statuses"})
async def get_statuses(
    txn: Transaction, *, kbid: str, rid: str, fields: Sequence[rpb2.FieldID]
) -> list[wpb2.FieldStatus]:
    result = []
    for field in fields:
        status = await get_status(
            txn, kbid=kbid, rid=rid, field_type=_to_abbr(field.field_type), field_id=field.field
        )
        result.append(status if status is not None else wpb2.FieldStatus())
    return result


def _to_abbr(field_type: rpb2.FieldType.ValueType) -> str:
    return from_proto.field_type_name(field_type).abbreviation()


async def _field_uris(
    txn: Transaction, kbid: str, rid: str | None = None, md5: str | None = None
) -> list[str]:
    clauses = [f"cts.collectionQuery({json.dumps(MarkLogicCollections.FIELDS)})"]
    if rid is not None:
        clauses.append(
            f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.RID)}, '=', {json.dumps(rid)})"
        )
    if md5 is not None:
        clauses.append(
            f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MD5)}, '=', {json.dumps(md5)})"
        )
    javascript = f"cts.uris('', ['document', 'item-order'], cts.andQuery([{', '.join(clauses)}]))"
    return [str(uri) for uri in await documents.evaluate(txn, await _database(txn, kbid), javascript)]


@observer.wrap({"type": "field", "op": "get_all_field_ids"})
async def get_all_field_ids(txn: Transaction, *, kbid: str, rid: str) -> rpb2.AllFieldIDs:
    database = await _database(txn, kbid)
    result = rpb2.AllFieldIDs()
    for uri in await _field_uris(txn, kbid, rid=rid):
        content = await documents.read(txn, database, uri)
        if content is None:
            continue
        field_type, field_id = content["field_type"], content["field_id"]
        if field_type == "a" and field_id in ("title", "summary"):
            continue
        field = result.fields.add()
        field.field_type = to_proto.field_type(field_type)
        field.field = field_id
    return result


@observer.wrap({"type": "field", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str, rid: str, field_id: rpb2.FieldID) -> bool:
    uri = _uri(rid, _to_abbr(field_id.field_type), field_id.field)
    return await documents.read(txn, await _database(txn, kbid), uri) is not None


@observer.wrap({"type": "field", "op": "exists_md5"})
async def exists_md5(txn: Transaction, *, kbid: str, md5: str, field_type: str) -> bool:
    database = await _database(txn, kbid)
    for uri in await _field_uris(txn, kbid, md5=md5):
        content = await documents.read(txn, database, uri)
        if content is not None and content["field_type"] == field_type:
            return True
    return False


@observer.wrap({"type": "field", "op": "set_md5"})
async def set_md5(
    txn: Transaction, *, kbid: str, md5: str, rid: str, field_id: str, field_type: str
) -> None:
    await _update(txn, kbid, rid, field_type, field_id, md5=md5)
