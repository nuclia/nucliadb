# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
"""MarkLogic datamanager for KB resources."""

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Final, Literal, TypeAlias, cast

from marklogic.documents import Document  # type: ignore[import-untyped]
from typing_extensions import assert_never

from nucliadb.common.datamanagers.utils import (
    UNSET,
    _UnsetType,
    observer,
    with_ro_transaction,
)
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.exceptions import ConflictError, NotFoundError
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb_models.filters import And, Label, Not, Or
from nucliadb_protos import resources_pb2

ResourceColumn: TypeAlias = Literal["slug", "shard", "basic", "origin", "security", "extra", "labels"]
LabelFilter: TypeAlias = Label | And | Or | Not
ALL_COLUMNS: tuple[ResourceColumn, ...] = (
    "slug",
    "shard",
    "basic",
    "origin",
    "security",
    "extra",
    "labels",
)

UNSET_STR: Final[str | None] = cast(str | None, UNSET)
UNSET_BASIC: Final[resources_pb2.Basic | None] = cast(resources_pb2.Basic | None, UNSET)
UNSET_ORIGIN: Final[resources_pb2.Origin | None] = cast(resources_pb2.Origin | None, UNSET)
UNSET_SECURITY: Final[resources_pb2.Security | None] = cast(resources_pb2.Security | None, UNSET)
UNSET_EXTRA: Final[resources_pb2.Extra | None] = cast(resources_pb2.Extra | None, UNSET)
UNSET_LABELS: Final[list[str] | None] = cast(list[str] | None, UNSET)


@dataclass(slots=True)
class ResourceData:
    slug: str | None = UNSET_STR
    shard: str | None = UNSET_STR
    basic: resources_pb2.Basic | None = UNSET_BASIC
    origin: resources_pb2.Origin | None = UNSET_ORIGIN
    security: resources_pb2.Security | None = UNSET_SECURITY
    extra: resources_pb2.Extra | None = UNSET_EXTRA
    labels: list[str] | None = UNSET_LABELS


ResourceColumnValueType = (
    str
    | None
    | resources_pb2.Basic
    | resources_pb2.Origin
    | resources_pb2.Security
    | resources_pb2.Extra
)
SerializedResourceColumnValueType = str | None | bytes


def _serialize_resource_column(
    value: _UnsetType | ResourceColumnValueType,
) -> _UnsetType | SerializedResourceColumnValueType:
    if isinstance(value, _UnsetType):
        return UNSET
    elif value is None:
        return None
    elif isinstance(value, str):
        return value
    elif isinstance(value, resources_pb2.Basic):
        return value.SerializeToString()
    elif isinstance(value, resources_pb2.Origin):
        return value.SerializeToString()
    elif isinstance(value, resources_pb2.Security):
        return value.SerializeToString()
    elif isinstance(value, resources_pb2.Extra):
        return value.SerializeToString()
    else:  # pragma: no cover
        assert_never(value)


def _deserialize_resource_column(
    column: ResourceColumn, value: SerializedResourceColumnValueType
) -> ResourceColumnValueType:
    if value is None:
        return None
    if column == "slug":
        return str(value)
    elif column == "shard":
        return str(value)
    elif column == "basic":
        assert isinstance(value, bytes)
        pb = resources_pb2.Basic()
        pb.ParseFromString(value)
        return pb
    elif column == "origin":
        assert isinstance(value, bytes)
        pb_origin = resources_pb2.Origin()
        pb_origin.ParseFromString(value)
        return pb_origin
    elif column == "security":
        assert isinstance(value, bytes)
        pb_security = resources_pb2.Security()
        pb_security.ParseFromString(value)
        return pb_security
    elif column == "extra":
        assert isinstance(value, bytes)
        pb_extra = resources_pb2.Extra()
        pb_extra.ParseFromString(value)
        return pb_extra
    elif column == "labels":
        raise ValueError("Labels are read directly from the resource document")
    else:  # pragma: no cover
        assert_never(column)


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------


@observer.wrap({"type": "resources", "op": "set"})
async def set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    shard: str | None | _UnsetType = UNSET,
    basic: resources_pb2.Basic | None | _UnsetType = UNSET,
    origin: resources_pb2.Origin | None | _UnsetType = UNSET,
    security: resources_pb2.Security | None | _UnsetType = UNSET,
    extra: resources_pb2.Extra | None | _UnsetType = UNSET,
    labels: list[str] | None | _UnsetType = UNSET,
) -> None:
    return await _set(
        txn,
        kbid=kbid,
        rid=rid,
        shard=shard,
        basic=basic,
        origin=origin,
        security=security,
        extra=extra,
        labels=labels,
    )


async def _set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    slug: str | None | _UnsetType = UNSET,
    shard: str | None | _UnsetType = UNSET,
    basic: resources_pb2.Basic | None | _UnsetType = UNSET,
    origin: resources_pb2.Origin | None | _UnsetType = UNSET,
    security: resources_pb2.Security | None | _UnsetType = UNSET,
    extra: resources_pb2.Extra | None | _UnsetType = UNSET,
    labels: list[str] | None | _UnsetType = UNSET,
) -> None:
    """Upsert only explicitly provided resource fields."""
    driver, marklogic_txn = _driver_txn(txn)
    values = {
        "slug": _serialize_resource_column(slug),
        "shard": _serialize_resource_column(shard),
        "basic": _serialize_resource_column(basic),
        "origin": _serialize_resource_column(origin),
        "security": _serialize_resource_column(security),
        "extra": _serialize_resource_column(extra),
    }
    columns_to_set = [
        column_name
        for column_name in ("slug", "shard", "basic", "origin", "security", "extra")
        if values[column_name] is not UNSET
    ]
    if not columns_to_set and labels is UNSET:
        return

    content = _content(await _read(driver, marklogic_txn, kbid, rid)) or {}
    content.update(resource_kbid=kbid, rid=rid)
    for column in columns_to_set:
        value = values[column]
        property_name = "resource_shard" if column == "shard" else column
        content[property_name] = (
            base64.b64encode(value).decode("ascii") if isinstance(value, bytes) else value
        )
    if "slug" in columns_to_set:
        content["resource_slug"] = content["slug"]
    if "basic" in columns_to_set:
        content["resource_title"] = basic.title if isinstance(basic, resources_pb2.Basic) else None
        content["labels"] = (
            sorted({f"/l/{item.labelset}/{item.label}" for item in basic.usermetadata.classifications})
            if isinstance(basic, resources_pb2.Basic)
            else []
        )
    if labels is None:
        content["labels"] = []
    elif isinstance(labels, list):
        explicit_labels = cast(list[str], labels)
        if any(not label.startswith("/l/") for label in explicit_labels):
            raise ValueError("Resource labels must use /l/<labelset>/<label> paths")
        content["labels"] = sorted({label for label in explicit_labels})
    response = await asyncio.to_thread(
        driver.client.documents.write,
        Document(
            uri=_uri(kbid, rid),
            content=content,
            collections=[MarkLogicCollections.RESOURCES],
            content_type="application/json",
        ),
        tx=marklogic_txn.transaction,
        params={"database": driver.database},
    )
    if not response.ok:
        raise RuntimeError(f"Failed to write resource: {response.status_code} {response.text}")


def _driver_txn(txn: Transaction) -> tuple[MarkLogicDriver, MarkLogicTransaction]:
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver) or not isinstance(txn, MarkLogicTransaction):
        raise TypeError("Resource datamanager requires MarkLogicDriver")
    return driver, txn


def _uri(kbid: str, rid: str) -> str:
    return f"resources/{kbid}/{rid}.json"


def _content(document: Document | None) -> dict | None:
    if document is None:
        return None
    if not isinstance(document.content, dict):
        raise RuntimeError(f"Invalid resource document: {document.uri}")
    return dict(document.content)


async def _read(
    driver: MarkLogicDriver, txn: MarkLogicTransaction, kbid: str, rid: str
) -> Document | None:
    params = {"database": driver.database}
    if txn.transaction is not None:
        params["txid"] = txn.transaction.id
    result = await asyncio.to_thread(
        driver.client.documents.read,
        _uri(kbid, rid),
        tx=txn.transaction,
        params=params,
    )
    return result[0] if isinstance(result, list) and result else None


@observer.wrap({"type": "resources", "op": "set_slug"})
async def set_slug(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    slug: str,
) -> None:
    existing = await _find_rid(txn, kbid, slug)
    if existing is not None and existing != rid:
        raise ConflictError(f"Slug '{slug}' already exists")
    await _set(txn, kbid=kbid, rid=rid, slug=slug)


@observer.wrap({"type": "resources", "op": "update_slug"})
async def update_slug(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    new_slug: str,
) -> str:
    """
    NOTE: Slug is stored twice (in the slug column and in the basic column).
    This function makes sure to update both in a single transaction.
    Ideally we should only store it in the slug column.
    """
    data = await _get(txn, kbid=kbid, rid=rid, columns=("basic", "slug"), for_update=True)
    if (
        data is None
        or data.basic is None
        or data.basic is UNSET
        or data.slug is None
        or data.slug is UNSET
    ):
        raise NotFoundError()
    old_slug = data.slug
    basic = data.basic
    basic.slug = new_slug
    existing = await _find_rid(txn, kbid, new_slug)
    if existing is not None and existing != rid:
        raise ConflictError(f"Slug '{new_slug}' already exists")
    await _set(txn, kbid=kbid, rid=rid, slug=new_slug, basic=basic)
    return old_slug


@observer.wrap({"type": "resources", "op": "delete"})
async def delete(txn: Transaction, *, kbid: str, rid: str) -> None:
    driver, marklogic_txn = _driver_txn(txn)
    params = {"database": driver.database, "uri": _uri(kbid, rid)}
    if marklogic_txn.transaction is not None:
        params["txid"] = marklogic_txn.transaction.id
    response = await asyncio.to_thread(driver.client.delete, "/v1/documents", params=params)
    if not response.ok:
        raise RuntimeError(f"Failed to delete resource: {response.status_code} {response.text}")


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


@observer.wrap({"type": "resources", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str, rid: str) -> bool:
    driver, marklogic_txn = _driver_txn(txn)
    return await _read(driver, marklogic_txn, kbid, rid) is not None


@observer.wrap({"type": "resources", "op": "get_rid"})
async def get_rid(txn: Transaction, *, kbid: str, slug: str) -> str | None:
    return await _find_rid(txn, kbid, slug)


async def _find_rid(txn: Transaction, kbid: str, slug: str) -> str | None:
    driver, marklogic_txn = _driver_txn(txn)
    javascript = (
        "cts.uris('', ['document'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)}), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.RESOURCE_KBID)}, '=', {json.dumps(kbid)}), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.RESOURCE_SLUG)}, '=', {json.dumps(slug)})]))"
    )
    params = {"database": driver.database}
    if marklogic_txn.transaction is not None:
        params["txid"] = marklogic_txn.transaction.id
    uris = await asyncio.to_thread(driver.client.eval, javascript=javascript, params=params) or []
    return str(uris[0]).rsplit("/", 1)[-1].removesuffix(".json") if uris else None


@observer.wrap({"type": "resources", "op": "get_by_slug"})
async def get_by_slug(txn: Transaction, *, kbid: str, slug: str) -> ResourceData | None:
    rid = await _find_rid(txn, kbid, slug)
    return await _get(txn, kbid=kbid, rid=rid, columns=ALL_COLUMNS) if rid is not None else None


def _label_query(expression: LabelFilter) -> str:
    if isinstance(expression, Label):
        facet = f"/l/{expression.labelset}"
        if expression.label:
            facet += f"/{expression.label}"
        return (
            f"cts.jsonPropertyValueQuery('labels', {json.dumps(facet)}, ['exact'])"
            if expression.label
            else (
                "cts.orQuery(["
                f"cts.jsonPropertyValueQuery('labels', {json.dumps(facet)}, ['exact']), "
                f"cts.jsonPropertyValueQuery('labels', {json.dumps(facet + '/*')}, ['wildcarded'])])"
            )
        )
    if isinstance(expression, And):
        return f"cts.andQuery([{', '.join(_label_query(cast(LabelFilter, operand)) for operand in expression.operands)}])"
    if isinstance(expression, Or):
        return f"cts.orQuery([{', '.join(_label_query(cast(LabelFilter, operand)) for operand in expression.operands)}])"
    if isinstance(expression, Not):
        return f"cts.notQuery({_label_query(cast(LabelFilter, expression.operand))})"
    raise TypeError(f"Unsupported resource label filter: {type(expression)}")


def _kb_query(kbid: str) -> list[str]:
    return [
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)})",
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.RESOURCE_KBID)}, '=', {json.dumps(kbid)})",
    ]


@observer.wrap({"type": "resources", "op": "search"})
async def search(
    txn: Transaction,
    *,
    kbid: str,
    title: str | None = None,
    slug: str | None = None,
    labels: LabelFilter | None = None,
) -> list[tuple[str, ResourceData]]:
    driver, marklogic_txn = _driver_txn(txn)
    clauses = _kb_query(kbid)
    for path, value in (
        (MarkLogicIndexPaths.RESOURCE_TITLE, title),
        (MarkLogicIndexPaths.RESOURCE_SLUG, slug),
    ):
        if value is not None:
            clauses.append(
                f"cts.jsonPropertyValueQuery({json.dumps(path.lstrip('/'))}, "
                f"{json.dumps('*' + value + '*')}, ['wildcarded'])"
            )
    if labels is not None:
        clauses.append(_label_query(labels))
    javascript = f"cts.uris('', ['document', 'item-order'], cts.andQuery([{', '.join(clauses)}]))"
    params = {"database": driver.database}
    if marklogic_txn.transaction is not None:
        params["txid"] = marklogic_txn.transaction.id
    uris = await asyncio.to_thread(driver.client.eval, javascript=javascript, params=params) or []
    results: list[tuple[str, ResourceData]] = []
    for uri in uris:
        rid = str(uri).rsplit("/", 1)[-1].removesuffix(".json")
        resource = await _get(txn, kbid=kbid, rid=rid, columns=ALL_COLUMNS)
        if resource is not None:
            results.append((rid, resource))
    return results


@observer.wrap({"type": "resources", "op": "label_facets"})
async def label_facets(txn: Transaction, *, kbid: str) -> dict[str, int]:
    driver, marklogic_txn = _driver_txn(txn)
    javascript = (
        "cts.values(cts.jsonPropertyReference('labels'), null, ['item-order'], "
        f"cts.andQuery([{', '.join(_kb_query(kbid))}])).toArray().map(String)"
    )
    params = {"database": driver.database}
    if marklogic_txn.transaction is not None:
        params["txid"] = marklogic_txn.transaction.id
    values = await asyncio.to_thread(driver.client.eval, javascript=javascript, params=params) or []
    if len(values) == 1 and isinstance(values[0], list):
        values = values[0]
    facets = {"/l"}
    for value in values:
        parts = str(value).split("/")
        if len(parts) >= 4 and parts[1] == "l":
            facets.update("/".join(parts[:index]) for index in range(3, len(parts) + 1))
    counts: dict[str, int] = {}
    for facet in sorted(facets):
        query = (
            "cts.orQuery(["
            f"cts.jsonPropertyValueQuery('labels', {json.dumps(facet)}, ['exact']), "
            f"cts.jsonPropertyValueQuery('labels', {json.dumps(facet + '/*')}, ['wildcarded'])])"
        )
        javascript = f"cts.estimate(cts.andQuery([{', '.join([*_kb_query(kbid), query])}]))"
        result = await asyncio.to_thread(driver.client.eval, javascript=javascript, params=params)
        if isinstance(result, list):
            result = result[0] if result else 0
        if result:
            counts[facet] = int(result)
    return counts


@observer.wrap({"type": "resources", "op": "slug_exists"})
async def slug_exists(txn: Transaction, *, kbid: str, slug: str) -> bool:
    return await _find_rid(txn, kbid, slug) is not None


@observer.wrap({"type": "resources", "op": "get_basic"})
async def get_basic(
    txn: Transaction, *, kbid: str, rid: str, for_update: bool = False
) -> resources_pb2.Basic | None:
    resource = await _get(txn, kbid=kbid, rid=rid, columns=("basic",), for_update=for_update)
    return resource.basic if resource is not None else None


@observer.wrap({"type": "resources", "op": "iter"})
async def iter(*, kbid: str) -> AsyncIterator[str]:
    async with with_ro_transaction() as txn:
        driver, marklogic_txn = _driver_txn(txn)
        uris = await _resource_uris(driver, marklogic_txn, kbid)
        for uri in sorted(str(uri) for uri in uris):
            yield uri.rsplit("/", 1)[-1].removesuffix(".json")


@observer.wrap({"type": "resources", "op": "count"})
async def count(txn: Transaction, *, kbid: str) -> int:
    driver, marklogic_txn = _driver_txn(txn)
    javascript = f"cts.estimate(cts.andQuery([{', '.join(_kb_query(kbid))}]))"
    params = {"database": driver.database}
    if marklogic_txn.transaction is not None:
        params["txid"] = marklogic_txn.transaction.id
    result = await asyncio.to_thread(driver.client.eval, javascript=javascript, params=params)
    if isinstance(result, list):
        result = result[0] if result else 0
    return int(result or 0)


def _shard_query(kbid: str, shard_id: str) -> list[str]:
    return [
        *_kb_query(kbid),
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.RESOURCE_SHARD)}, '=', {json.dumps(shard_id)})",
    ]


@observer.wrap({"type": "resources", "op": "get_resources_from_shard"})
async def get_resources_from_shard(
    txn: Transaction, *, kbid: str, shard_id: str, limit: int
) -> list[str]:
    if limit <= 0:
        return []
    driver, marklogic_txn = _driver_txn(txn)
    javascript = (
        f"cts.uris('', {json.dumps(['document', 'item-order', f'limit={limit}'])}, "
        f"cts.andQuery([{', '.join(_shard_query(kbid, shard_id))}]))"
    )
    params = {"database": driver.database}
    if marklogic_txn.transaction is not None:
        params["txid"] = marklogic_txn.transaction.id
    uris = await asyncio.to_thread(driver.client.eval, javascript=javascript, params=params) or []
    return [str(uri).rsplit("/", 1)[-1].removesuffix(".json") for uri in uris]


@observer.wrap({"type": "resources", "op": "count_resources_in_shard"})
async def count_resources_in_shard(txn: Transaction, *, kbid: str, shard_id: str) -> int:
    driver, marklogic_txn = _driver_txn(txn)
    javascript = f"cts.estimate(cts.andQuery([{', '.join(_shard_query(kbid, shard_id))}]))"
    params = {"database": driver.database}
    if marklogic_txn.transaction is not None:
        params["txid"] = marklogic_txn.transaction.id
    result = await asyncio.to_thread(driver.client.eval, javascript=javascript, params=params)
    if isinstance(result, list):
        result = result[0] if result else 0
    return int(result or 0)


async def _resource_uris(driver: MarkLogicDriver, txn: MarkLogicTransaction, kbid: str) -> list[str]:
    javascript = (
        "cts.uris('', ['document', 'item-order'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)}), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.RESOURCE_KBID)}, '=', {json.dumps(kbid)})]))"
    )
    params = {"database": driver.database}
    if txn.transaction is not None:
        params["txid"] = txn.transaction.id
    return await asyncio.to_thread(driver.client.eval, javascript=javascript, params=params) or []


@observer.wrap({"type": "resources", "op": "get_shard"})
async def get_shard(txn: Transaction, *, kbid: str, rid: str, for_update: bool = False) -> str | None:
    resource = await _get(txn, kbid=kbid, rid=rid, columns=("shard",), for_update=for_update)
    if resource is None:
        return None
    assert resource.shard is not UNSET
    return resource.shard


@observer.wrap({"type": "resources", "op": "get_shards"})
async def get_shards(txn: Transaction, *, kbid: str, rids: list[str]) -> dict[str, str]:
    if not rids:
        return {}
    driver, marklogic_txn = _driver_txn(txn)
    result = await asyncio.to_thread(
        driver.client.documents.read,
        [_uri(kbid, rid) for rid in rids],
        tx=marklogic_txn.transaction,
        params={"database": driver.database},
    )
    if not isinstance(result, list):
        driver.data._check(result, "read resource shards")
        raise RuntimeError("Unexpected response when reading resource shards")
    return {
        str(document.uri).rsplit("/", 1)[-1].removesuffix(".json"): content["resource_shard"]
        for document in result
        if (content := _content(document)) is not None and content.get("resource_shard") is not None
    }


@observer.wrap({"type": "resources", "op": "get"})
async def get(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    columns: tuple[ResourceColumn, ...],
    for_update: bool = False,
) -> ResourceData | None:
    """Return the selected resource columns for a row, or None if the row does not exist.

    Non-requested fields are left as UNSET. Requested null values are returned as None.
    """
    return await _get(txn, kbid=kbid, rid=rid, columns=columns, for_update=for_update)


async def _get(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    columns: tuple[ResourceColumn, ...],
    for_update: bool = False,
) -> ResourceData | None:
    """Return the selected resource columns for a row, or None if the row does not exist.

    Non-requested fields are left as UNSET. Requested null values are returned as None.
    """
    if not columns:
        raise ValueError("At least one resource column must be requested")

    driver, marklogic_txn = _driver_txn(txn)
    content = _content(await _read(driver, marklogic_txn, kbid, rid))
    if content is None:
        return None
    resource = ResourceData()
    for column_name in columns:
        value = content.get("resource_shard" if column_name == "shard" else column_name)
        if column_name == "labels":
            resource.labels = value
            continue
        if value is not None and column_name in ("basic", "origin", "security", "extra"):
            value = base64.b64decode(value)
        setattr(resource, column_name, _deserialize_resource_column(column_name, value))
    return resource
