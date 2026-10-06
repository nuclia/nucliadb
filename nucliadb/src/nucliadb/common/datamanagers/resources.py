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

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Final, Literal, TypeAlias, cast

from typing_extensions import assert_never

from nucliadb.common.datamanagers import marklogic_documents as documents
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
from nucliadb_protos import resources_pb2

ResourceColumn: TypeAlias = Literal["slug", "basic", "origin", "security", "extra"]
RESOURCE_URI_PAGE_SIZE = 1000
ALL_COLUMNS: tuple[ResourceColumn, ...] = (
    "slug",
    "basic",
    "origin",
    "security",
    "extra",
)

UNSET_STR: Final[str | None] = cast(str | None, UNSET)
UNSET_BASIC: Final[resources_pb2.Basic | None] = cast(resources_pb2.Basic | None, UNSET)
UNSET_ORIGIN: Final[resources_pb2.Origin | None] = cast(resources_pb2.Origin | None, UNSET)
UNSET_SECURITY: Final[resources_pb2.Security | None] = cast(resources_pb2.Security | None, UNSET)
UNSET_EXTRA: Final[resources_pb2.Extra | None] = cast(resources_pb2.Extra | None, UNSET)


@dataclass(slots=True)
class ResourceData:
    slug: str | None = UNSET_STR
    basic: resources_pb2.Basic | None = UNSET_BASIC
    origin: resources_pb2.Origin | None = UNSET_ORIGIN
    security: resources_pb2.Security | None = UNSET_SECURITY
    extra: resources_pb2.Extra | None = UNSET_EXTRA


ResourceColumnValueType = (
    str
    | None
    | resources_pb2.Basic
    | resources_pb2.Origin
    | resources_pb2.Security
    | resources_pb2.Extra
)
SerializedResourceColumnValueType = str | None | dict


def _serialize_resource_column(
    value: _UnsetType | ResourceColumnValueType,
) -> _UnsetType | SerializedResourceColumnValueType:
    if isinstance(value, _UnsetType):
        return UNSET
    elif value is None:
        return None
    elif isinstance(value, str):
        return value
    elif isinstance(
        value,
        (resources_pb2.Basic, resources_pb2.Origin, resources_pb2.Security, resources_pb2.Extra),
    ):
        return documents.to_json(value)
    else:  # pragma: no cover
        assert_never(value)


def _deserialize_resource_column(
    column: ResourceColumn, value: SerializedResourceColumnValueType
) -> ResourceColumnValueType:
    if value is None:
        return None
    if column == "slug":
        return str(value)
    elif column == "basic":
        assert isinstance(value, dict)
        return documents.from_json(value, resources_pb2.Basic)
    elif column == "origin":
        assert isinstance(value, dict)
        return documents.from_json(value, resources_pb2.Origin)
    elif column == "security":
        assert isinstance(value, dict)
        return documents.from_json(value, resources_pb2.Security)
    elif column == "extra":
        assert isinstance(value, dict)
        return documents.from_json(value, resources_pb2.Extra)
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
    basic: resources_pb2.Basic | None | _UnsetType = UNSET,
    origin: resources_pb2.Origin | None | _UnsetType = UNSET,
    security: resources_pb2.Security | None | _UnsetType = UNSET,
    extra: resources_pb2.Extra | None | _UnsetType = UNSET,
) -> None:
    return await _set(
        txn,
        kbid=kbid,
        rid=rid,
        basic=basic,
        origin=origin,
        security=security,
        extra=extra,
    )


async def _set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    slug: str | None | _UnsetType = UNSET,
    basic: resources_pb2.Basic | None | _UnsetType = UNSET,
    origin: resources_pb2.Origin | None | _UnsetType = UNSET,
    security: resources_pb2.Security | None | _UnsetType = UNSET,
    extra: resources_pb2.Extra | None | _UnsetType = UNSET,
) -> None:
    # TODO(Marklogic): Implement proper upsert logic for MarkLogic, avoiding read-modify-write cycles.
    driver, _ = documents.driver_txn(txn)
    values = {
        "slug": _serialize_resource_column(slug),
        "basic": _serialize_resource_column(basic),
        "origin": _serialize_resource_column(origin),
        "security": _serialize_resource_column(security),
        "extra": _serialize_resource_column(extra),
    }
    columns_to_set = [
        column_name
        for column_name in ("slug", "basic", "origin", "security", "extra")
        if values[column_name] is not UNSET
    ]
    if not columns_to_set:
        return
    database = driver.kb_database(kbid)
    uri = _uri(rid)
    content = await documents.read(txn, database, uri)
    if content is None:
        content = {}
    for column in columns_to_set:
        content[column] = values[column]
    await documents.write(txn, database, uri, MarkLogicCollections.RESOURCES, content)


def _uri(rid: str) -> str:
    return f"/resources/{rid}.json"


def _database(txn: Transaction, kbid: str) -> str:
    driver, _ = documents.driver_txn(txn)
    return driver.kb_database(kbid)


@observer.wrap({"type": "resources", "op": "set_slug"})
async def set_slug(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    slug: str,
) -> None:
    existing = await _get_rid_by_slug(txn, kbid, slug)
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
    existing = await _get_rid_by_slug(txn, kbid, new_slug)
    if existing is not None and existing != rid:
        raise ConflictError(f"Slug '{new_slug}' already exists")
    await _set(txn, kbid=kbid, rid=rid, slug=new_slug, basic=basic)
    return old_slug


@observer.wrap({"type": "resources", "op": "delete"})
async def delete(txn: Transaction, *, kbid: str, rid: str) -> None:
    # TODO(Marklogic): Implement directory delete here
    database = _database(txn, kbid)
    await documents.delete_resource_children(txn, database, rid)
    await documents.delete(txn, database, _uri(rid))


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


@observer.wrap({"type": "resources", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str, rid: str) -> bool:
    return await documents.exists(txn, _database(txn, kbid), _uri(rid))


@observer.wrap({"type": "resources", "op": "get_rid"})
async def get_rid(txn: Transaction, *, kbid: str, slug: str) -> str | None:
    return await _get_rid_by_slug(txn, kbid, slug)


async def _get_rid_by_slug(txn: Transaction, kbid: str, slug: str) -> str | None:
    javascript = (
        "cts.uris('', ['document'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)}), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.SLUG)}, '=', {json.dumps(slug)})]))"
    )
    uris = await documents.evaluate(txn, _database(txn, kbid), javascript)
    return _rid_from_uri(uris[0]) if uris else None


def _rid_from_uri(uri: object) -> str:
    """
    Converts a document URI to a resource ID (rid).

    >>> _rid_from_uri("/resources/123.json")
    '123'
    """
    return str(uri).rsplit("/", 1)[-1].removesuffix(".json")


def _resources_query() -> list[str]:
    return [f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)})"]


@observer.wrap({"type": "resources", "op": "slug_exists"})
async def slug_exists(txn: Transaction, *, kbid: str, slug: str) -> bool:
    return await _get_rid_by_slug(txn, kbid, slug) is not None


@observer.wrap({"type": "resources", "op": "get_basic"})
async def get_basic(
    txn: Transaction, *, kbid: str, rid: str, for_update: bool = False
) -> resources_pb2.Basic | None:
    resource = await _get(txn, kbid=kbid, rid=rid, columns=("basic",), for_update=for_update)
    return resource.basic if resource is not None else None


@observer.wrap({"type": "resources", "op": "iter"})
async def iter(*, kbid: str) -> AsyncIterator[str]:
    async with with_ro_transaction(kbid=kbid) as txn:
        start_uri: str | None = None
        while True:
            page_limit = RESOURCE_URI_PAGE_SIZE + (start_uri is not None)
            uris = await _resource_uris(txn, kbid, start_uri=start_uri, limit=page_limit)
            has_more = len(uris) == page_limit
            if start_uri is not None and uris and uris[0] == start_uri:
                # Avoid yielding the start_uri again
                uris = uris[1:]
            if not uris:
                return
            for uri in uris:
                yield _rid_from_uri(uri)
            if not has_more:
                return
            start_uri = uris[-1]


@observer.wrap({"type": "resources", "op": "count"})
async def count(txn: Transaction, *, kbid: str) -> int:
    # TODO(Marklogic): Validate that this is the right way to count docs of a particular collection (or uri scheme)
    javascript = f"cts.estimate(cts.andQuery([{', '.join(_resources_query())}]))"
    result = await documents.evaluate(txn, _database(txn, kbid), javascript)
    return int(result[0] if result else 0)


async def _resource_uris(
    txn: Transaction, kbid: str, *, start_uri: str | None = None, limit: int = RESOURCE_URI_PAGE_SIZE
) -> list[str]:
    start = json.dumps(start_uri or "")
    javascript = (
        f"fn.subsequence(cts.uris({start}, ['document', 'item-order'], "
        f"cts.andQuery([{', '.join(_resources_query())}])), 1, {limit})"
    )
    return [str(uri) for uri in await documents.evaluate(txn, _database(txn, kbid), javascript)]


@observer.wrap({"type": "resources", "op": "get"})
async def get(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    columns: tuple[ResourceColumn, ...],
    for_update: bool = False,
) -> ResourceData | None:
    return await _get(txn, kbid=kbid, rid=rid, columns=columns, for_update=for_update)


async def _get(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    columns: tuple[ResourceColumn, ...],
    for_update: bool = False,
) -> ResourceData | None:
    # TODO(Marklogic): Use optic to select only the requested columns instead of reading the entire document
    if not columns:
        raise ValueError("At least one resource column must be requested")
    content = await documents.read(txn, _database(txn, kbid), _uri(rid))
    if content is None:
        return None
    resource = ResourceData()
    for column_name in columns:
        value = content.get(column_name)
        setattr(resource, column_name, _deserialize_resource_column(column_name, value))
    return resource
