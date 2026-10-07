"""MarkLogic datamanager for resource fields."""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal, TypeAlias, TypeVar, cast
from urllib.parse import quote, unquote

from google.protobuf.message import Message

from nucliadb.common.datamanagers import marklogic_documents as documents
from nucliadb.common.datamanagers.utils import UNSET, observer
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.models_utils import from_proto, to_proto
from nucliadb_protos import resources_pb2 as rpb2
from nucliadb_protos import writer_pb2 as wpb2

PB = TypeVar("PB", bound=Message)
FieldColumn: TypeAlias = Literal["value", "status", "md5"]
UNSET_VALUE: Final[Message | None] = cast(Message | None, UNSET)
UNSET_STATUS: Final[wpb2.FieldStatus | None] = cast(wpb2.FieldStatus | None, UNSET)
UNSET_MD5: Final[str | None] = cast(str | None, UNSET)


@dataclass(slots=True)
class FieldData:
    value: Message | None = UNSET_VALUE
    status: wpb2.FieldStatus | None = UNSET_STATUS
    md5: str | None = UNSET_MD5


def _uri(rid: str, field_type: str, field_id: str) -> str:
    return f"/resources/{rid}/fields/{field_type}/{quote(field_id, safe='')}.json"


async def _update(
    txn: Transaction, kbid: str, rid: str, field_type: str, field_id: str, **values: Any
) -> None:
    documents.require_kb_scope(txn, kbid)
    uri = _uri(rid, field_type, field_id)
    content = await documents.read(txn, uri) or {
        "rid": rid,
        "field_type": field_type,
        "field_id": field_id,
    }
    content.update(values)
    await documents.write(txn, uri, MarkLogicCollections.FIELDS, content)


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
    documents.require_kb_scope(txn, kbid)
    if field_type == "c":
        from nucliadb.common.datamanagers import conversations

        await conversations.delete_pages(txn, kbid=kbid, rid=rid, field_id=field_id)
    await documents.delete(txn, _uri(rid, field_type, field_id))


async def _get(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    columns: tuple[FieldColumn, ...],
    pb_klass: type[PB] | None = None,
) -> FieldData | None:
    documents.require_kb_scope(txn, kbid)
    if not columns:
        raise ValueError("At least one field column must be requested")
    if "value" in columns and pb_klass is None:
        raise ValueError("pb_klass is required when requesting the value column")
    content = await documents.read(txn, _uri(rid, field_type, field_id))
    if content is None:
        return None
    result = FieldData()
    for column in columns:
        value = content.get(column)
        if column == "value" and value is not None:
            value = documents.from_json(value, cast(type[PB], pb_klass))
        elif column == "status" and value is not None:
            value = documents.from_json(value, wpb2.FieldStatus)
        elif column == "md5" and value is not None:
            value = str(value)
        setattr(result, column, value)
    return result


@observer.wrap({"type": "field", "op": "get_value"})
async def get(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str, pb_klass: type[PB]
) -> PB | None:
    data = await _get(
        txn,
        kbid=kbid,
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        columns=("value",),
        pb_klass=pb_klass,
    )
    return cast(PB | None, data.value if data is not None else None)


@observer.wrap({"type": "field", "op": "get_status"})
async def get_status(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str
) -> wpb2.FieldStatus | None:
    data = await _get(
        txn,
        kbid=kbid,
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        columns=("status",),
    )
    return data.status if data is not None else None


@observer.wrap({"type": "field", "op": "get_statuses"})
async def get_statuses(
    txn: Transaction, *, kbid: str, rid: str, fields: Sequence[rpb2.FieldID]
) -> list[wpb2.FieldStatus]:
    documents.require_kb_scope(txn, kbid)
    result = []
    for field in fields:
        status = await get_status(
            txn, kbid=kbid, rid=rid, field_type=_to_abbr(field.field_type), field_id=field.field
        )
        result.append(status if status is not None else wpb2.FieldStatus())
    return result


def _to_abbr(field_type: rpb2.FieldType.ValueType) -> str:
    return from_proto.field_type_name(field_type).abbreviation()


async def _field_uris(txn: Transaction, kbid: str, rid: str) -> list[str]:
    documents.require_kb_scope(txn, kbid)
    clauses = [f"cts.collectionQuery({json.dumps(MarkLogicCollections.FIELDS)})"]
    clauses.append(f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.RID)}, '=', {json.dumps(rid)})")
    javascript = f"cts.uris('', ['document', 'item-order'], cts.andQuery([{', '.join(clauses)}]))"
    return [str(uri) for uri in await documents.evaluate(txn, javascript)]


def field_from_uri(uri: str) -> tuple[str, str]:
    """
    Convert from a field URI to a tuple of (field_type, field_id).

    >>> uri = "/resources/rid/fields/a/title.json"
    >>> field_from_uri(uri)
    ('a', 'title')
    """
    field_type, encoded_field_id = uri.rsplit("/", 2)[-2:]
    field_id = unquote(encoded_field_id.removesuffix(".json"))
    return field_type, field_id


@observer.wrap({"type": "field", "op": "get_all_field_ids"})
async def get_all_field_ids(txn: Transaction, *, kbid: str, rid: str) -> rpb2.AllFieldIDs:
    result = rpb2.AllFieldIDs()
    for uri in await _field_uris(txn, kbid, rid=rid):
        field_type, field_id = field_from_uri(uri)
        if field_type == "a" and field_id in ("title", "summary"):
            continue
        field = result.fields.add()
        field.field_type = to_proto.field_type(field_type)
        field.field = field_id
    return result


@observer.wrap({"type": "field", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str, rid: str, field_id: rpb2.FieldID) -> bool:
    documents.require_kb_scope(txn, kbid)
    uri = _uri(rid, _to_abbr(field_id.field_type), field_id.field)
    return await documents.exists(txn, uri)


@observer.wrap({"type": "field", "op": "exists_md5"})
async def exists_md5(txn: Transaction, *, kbid: str, md5: str, field_type: str) -> bool:
    documents.require_kb_scope(txn, kbid)
    javascript = (
        "const op = require('/MarkLogic/optic');"
        "op.fromDocUris(cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.FIELDS)}), "
        f"cts.jsonPropertyValueQuery('field_type', {json.dumps(field_type)}, ['exact']), "
        f"cts.jsonPropertyValueQuery('md5', {json.dumps(md5)}, ['exact'])"
        "])).limit(1).result()"
    )
    return bool(await documents.evaluate(txn, javascript))


@observer.wrap({"type": "field", "op": "set_md5"})
async def set_md5(
    txn: Transaction, *, kbid: str, md5: str, rid: str, field_id: str, field_type: str
) -> None:
    await _update(txn, kbid, rid, field_type, field_id, md5=md5)


"""
TODO(Marklogic):
- Make sure to leverage optic's patch feature to not do read-modify-write operations for updates
- Make sure listing fields is paginated
- Allow efficient gets by only selecting (with optic) the properties we need.
"""
