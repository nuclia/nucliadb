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
from typing import Any, Final, Literal, TypeAlias, cast

from pydantic import BaseModel
from typing_extensions import assert_never

from nucliadb.common.datamanagers.utils import (
    UNSET,
    _UnsetType,
    observer,
)
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.exceptions import ConflictError, NotFoundError
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.document_deletion import DocumentDeletion
from nucliadb.common.marklogic.document_update import DocumentUpdate
from nucliadb.common.marklogic.uris import MarkLogicURIs
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


CHILD_COLLECTIONS = (
    MarkLogicCollections.FIELDS,
    MarkLogicCollections.FIELDS_PROCESSED,
    MarkLogicCollections.CONVERSATIONS,
    MarkLogicCollections.CHUNKS,
    MarkLogicCollections.EMBEDDINGS,
)


class ResourceContent(BaseModel):
    slug: str
    basic: dict[str, Any]
    origin: dict[str, Any] | None = None
    extra: dict[str, Any] | None = None
    security: dict[str, Any] | None = None
    title: str | None = None
    processing_status: str
    created_at: str
    modified_at: str
    facets: list[str]


class ResourceUpdateContent(BaseModel):
    slug: str | None = None
    basic: dict[str, Any] | None = None
    origin: dict[str, Any] | None = None
    extra: dict[str, Any] | None = None
    security: dict[str, Any] | None = None
    title: str | None = None
    processing_status: str | None = None
    created_at: str | None = None
    modified_at: str | None = None
    facets: list[str] | None = None


class ResourceCreateContent(BaseModel):
    slug: str
    basic: dict[str, Any]
    origin: dict[str, Any] | None = None
    extra: dict[str, Any] | None = None
    security: dict[str, Any] | None = None
    title: str | None = None
    processing_status: str
    created_at: str
    modified_at: str
    facets: list[str] = []


def _driver_txn(
    txn: Transaction,
    kbid: str,
    ensure_writes: bool = False,
) -> tuple[MarkLogicDriver, MarkLogicTransaction]:
    marklogic_driver, marklogic_txn = documents.driver_txn(txn, ensure_writes=ensure_writes)
    assert marklogic_txn.kbid == kbid
    return marklogic_driver, marklogic_txn


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
    _, marklogic_txn = _driver_txn(txn, kbid=kbid, ensure_writes=True)
    values = {
        "slug": _serialize_resource_column(slug),
        "basic": _serialize_resource_column(basic),
        "origin": _serialize_resource_column(origin),
        "security": _serialize_resource_column(security),
        "extra": _serialize_resource_column(extra),
    }
    update = DocumentUpdate(uri=MarkLogicURIs.resource(rid), collection=MarkLogicCollections.RESOURCES)
    update.set(**{column: value for column, value in values.items() if value is not UNSET})
    await documents.update_document(marklogic_txn, update)


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
    """
    Deletes the resource with the given resource ID (rid) from the database, as well as all related child collections,
    like fields, chunks, embeddings, etc.
    """
    _, marklogic_txn = _driver_txn(txn, kbid=kbid, ensure_writes=True)
    deletion = (
        DocumentDeletion()
        .uri(MarkLogicURIs.resource(rid))
        .directory(MarkLogicURIs.resource_directory(rid), CHILD_COLLECTIONS)
    )
    await documents.delete_documents(marklogic_txn, deletion)


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


@observer.wrap({"type": "resources", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str, rid: str) -> bool:
    _, marklogic_txn = _driver_txn(txn, kbid=kbid)
    return await documents.exists(marklogic_txn, MarkLogicURIs.resource(rid))


@observer.wrap({"type": "resources", "op": "get_rid"})
async def get_rid(txn: Transaction, *, kbid: str, slug: str) -> str | None:
    return await _get_rid_by_slug(txn, kbid, slug)


async def _get_rid_by_slug(txn: Transaction, kbid: str, slug: str) -> str | None:
    _, marklogic_txn = _driver_txn(txn, kbid=kbid)
    javascript = (
        "cts.uris('', ['document'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)}), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.SLUG)}, '=', {json.dumps(slug)})]))"
    )
    uris = await documents.evaluate(marklogic_txn, javascript)
    return MarkLogicURIs.resource_id_from_uri(uris[0]) if uris else None


@observer.wrap({"type": "resources", "op": "get_resource_rid_by_slug"})
async def get_resource_rid_by_slug(driver: MarkLogicDriver, *, kbid: str, slug: str) -> str | None:
    async with driver.transaction(read_only=True, kbid=kbid) as txn:
        return await _get_rid_by_slug(txn, kbid, slug)


@observer.wrap({"type": "resources", "op": "get_resource"})
async def get_resource(txn: Transaction, *, kbid: str, rid: str) -> ResourceContent | None:
    _, marklogic_txn = _driver_txn(txn, kbid=kbid)
    content = await documents.read(marklogic_txn, MarkLogicURIs.resource(rid))
    return ResourceContent.model_validate(content) if content is not None else None


@observer.wrap({"type": "resources", "op": "create_resource"})
async def create_resource(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    data: ResourceCreateContent,
) -> None:
    _, marklogic_txn = _driver_txn(txn, kbid=kbid, ensure_writes=True)
    await documents.write(
        marklogic_txn,
        uri=MarkLogicURIs.resource(rid),
        collection=MarkLogicCollections.RESOURCES,
        content=data.model_dump(mode="json"),
    )


@observer.wrap({"type": "resources", "op": "update_resource"})
async def update_resource(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    data: ResourceContent | ResourceUpdateContent,
) -> bool:
    _, marklogic_txn = _driver_txn(txn, kbid=kbid, ensure_writes=True)
    update = DocumentUpdate(uri=MarkLogicURIs.resource(rid), collection=MarkLogicCollections.RESOURCES)
    update.set(**data.model_dump(mode="json", exclude_unset=True))
    return await documents.update_document(marklogic_txn, update)


@observer.wrap({"type": "resources", "op": "delete_resource"})
async def delete_resource(txn: Transaction, *, kbid: str, rid: str) -> None:
    await delete(txn, kbid=kbid, rid=rid)


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
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver):
        raise TypeError("Resource iteration requires MarkLogicDriver")
    async with driver.transaction(read_only=True, kbid=kbid) as txn:
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
                yield MarkLogicURIs.resource_id_from_uri(uri)
            if not has_more:
                return
            start_uri = uris[-1]


@observer.wrap({"type": "resources", "op": "count"})
async def count(txn: Transaction, *, kbid: str) -> int:
    _, marklogic_txn = _driver_txn(txn, kbid=kbid)
    # TODO(Marklogic): Validate that this is the right way to count docs of a particular collection (or uri scheme)
    javascript = f"cts.estimate(cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)}))"
    result = await documents.evaluate(marklogic_txn, javascript)
    return int(result[0] if result else 0)


@observer.wrap({"type": "resources", "op": "list_ids"})
async def list_ids(
    txn: Transaction,
    *,
    kbid: str,
    after_rid: str | None = None,
    limit: int = RESOURCE_URI_PAGE_SIZE,
) -> list[str]:
    """Page of resource ids in stable URI order, strictly after `after_rid` (which may no longer exist)."""
    start_uri = MarkLogicURIs.resource(after_rid) if after_rid is not None else None
    uris = await _resource_uris(txn, kbid, start_uri=start_uri, limit=limit + (start_uri is not None))
    if start_uri is not None and uris and uris[0] == start_uri:
        uris = uris[1:]
    return [MarkLogicURIs.resource_id_from_uri(uri) for uri in uris[:limit]]


async def _resource_uris(
    txn: Transaction, kbid: str, *, start_uri: str | None = None, limit: int = RESOURCE_URI_PAGE_SIZE
) -> list[str]:
    _, marklogic_txn = _driver_txn(txn, kbid=kbid)
    start = json.dumps(start_uri or "")
    javascript = (
        f"fn.subsequence(cts.uris({start}, ['document', 'item-order'], "
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)})"
        f"), 1, {limit})"
    )
    return [str(uri) for uri in await documents.evaluate(marklogic_txn, javascript)]


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
    _, marklogic_txn = _driver_txn(txn, kbid=kbid)
    if not columns:
        raise ValueError("At least one resource column must be requested")
    content = await documents.read(marklogic_txn, MarkLogicURIs.resource(rid))
    if content is None:
        return None
    resource = ResourceData()
    for column_name in columns:
        value = content.get(column_name)
        setattr(resource, column_name, _deserialize_resource_column(column_name, value))
    return resource
