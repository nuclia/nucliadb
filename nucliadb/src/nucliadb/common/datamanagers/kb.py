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
"""MarkLogic datamanager for KnowledgeBoxes."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final, Literal, TypeAlias, cast

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxConflict, KnowledgeBoxNotFound
from nucliadb.common.datamanagers.utils import UNSET, _UnsetType, observer
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.document_update import DocumentUpdate
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb_protos import knowledgebox_pb2, writer_pb2

logger = logging.getLogger(__name__)

KBColumn: TypeAlias = Literal["slug", "config", "shards"]

UNSET_STR: Final[str | None] = cast(str | None, UNSET)
UNSET_CONFIG: Final[knowledgebox_pb2.KnowledgeBoxConfig | None] = cast(
    knowledgebox_pb2.KnowledgeBoxConfig | None, UNSET
)
UNSET_SHARDS: Final[writer_pb2.Shards | None] = cast(writer_pb2.Shards | None, UNSET)


@dataclass(slots=True)
class KBData:
    """
    Data stored in the knowledgebox database.
    """

    slug: str | None = UNSET_STR
    config: knowledgebox_pb2.KnowledgeBoxConfig | None = UNSET_CONFIG
    shards: writer_pb2.Shards | None = UNSET_SHARDS


@dataclass(slots=True)
class KBRegistry:
    """
    Data stored in the system database.
    """

    kbid: str
    slug: str
    deleted_at: str | None = None


def ensure_system_scope(txn: MarkLogicTransaction):
    assert txn.is_system_scope


def _driver_txn(
    txn: Transaction, system_scope: bool, ensure_writes: bool = False, kbid: str | None = None
) -> tuple[MarkLogicDriver, MarkLogicTransaction]:
    marklogic_driver, marklogic_txn = documents.driver_txn(txn, ensure_writes=ensure_writes)
    assert marklogic_txn.is_system_scope == system_scope
    if kbid is not None:
        assert marklogic_txn.kbid == kbid
    return marklogic_driver, marklogic_txn


@observer.wrap({"type": "kb", "op": "set_registry"})
async def set_registry(
    txn: Transaction,
    *,
    kbid: str,
    slug: str,
) -> None:
    await _set_registry(
        txn,
        kbid=kbid,
        slug=slug,
    )


async def _set_registry(
    txn: Transaction,
    *,
    kbid: str,
    slug: str,
    deleted_at: str | None = None,
) -> None:
    _, marklogic_txn = _driver_txn(txn, system_scope=True, ensure_writes=True)
    await documents.write(
        marklogic_txn,
        MarkLogicURIs.kb_registry(kbid),
        MarkLogicCollections.KB_REGISTRY_ITEM,
        {"kbid": kbid, "slug": slug, "deleted_at": deleted_at},
    )


async def _get_registry(
    txn: Transaction,
    *,
    kbid: str,
) -> KBRegistry | None:
    _, marklogic_txn = _driver_txn(txn, system_scope=True)
    content = await documents.read(
        marklogic_txn,
        MarkLogicURIs.kb_registry(kbid),
    )
    if content is None:
        return None
    return KBRegistry(
        kbid=content["kbid"],
        slug=content["slug"],
        deleted_at=content.get("deleted_at"),
    )


def _serialize(value: knowledgebox_pb2.KnowledgeBoxConfig | writer_pb2.Shards | None | _UnsetType):
    if value is UNSET:
        return UNSET
    if value is None:
        return None
    if not isinstance(value, (knowledgebox_pb2.KnowledgeBoxConfig, writer_pb2.Shards)):
        raise ValueError(f"Unsupported KnowledgeBox column value: {type(value)}")
    return documents.to_json(value)


def _deserialize(column: KBColumn, value):
    if value is None:
        return None
    if column == "config":
        return documents.from_json(value, knowledgebox_pb2.KnowledgeBoxConfig)
    elif column == "shards":
        return documents.from_json(value, writer_pb2.Shards)
    else:  # Unknown column
        raise ValueError(f"Unsupported KB column: {column}")


@observer.wrap({"type": "kb", "op": "set"})
async def set(
    txn: Transaction,
    *,
    kbid: str,
    config: knowledgebox_pb2.KnowledgeBoxConfig | None | _UnsetType = UNSET,
    shards: writer_pb2.Shards | None | _UnsetType = UNSET,
) -> None:
    await _set_data(
        txn,
        kbid=kbid,
        config=config,
        shards=shards,
    )


async def _set_data(
    txn: Transaction,
    *,
    kbid: str,
    config: knowledgebox_pb2.KnowledgeBoxConfig | None | _UnsetType = UNSET,
    shards: writer_pb2.Shards | None | _UnsetType = UNSET,
) -> None:
    update = DocumentUpdate(
        uri=MarkLogicURIs.kb_data(),
        collection=MarkLogicCollections.KNOWLEDGEBOXES,
        defaults={"kbid": kbid},
    )
    for name, value in (("config", config), ("shards", shards)):
        if value is not UNSET:
            update[name] = _serialize(value)
    _, marklogic_txn = _driver_txn(txn, system_scope=False, ensure_writes=True, kbid=kbid)
    await documents.update_document(marklogic_txn, update)


@observer.wrap({"type": "kb", "op": "get"})
async def get(
    txn: Transaction,
    *,
    kbid: str,
    columns: tuple[KBColumn, ...],
    for_update: bool = False,
) -> KBData | None:
    return await _get_data(
        txn,
        kbid=kbid,
        columns=columns,
        for_update=for_update,
    )


async def _get_data(
    txn: Transaction,
    *,
    kbid: str,
    columns: tuple[KBColumn, ...],
    for_update: bool = False,
) -> KBData | None:
    if not columns:
        raise ValueError("At least one KB column must be requested")
    _, marklogic_txn = _driver_txn(txn, system_scope=False, kbid=kbid)
    content = await documents.read(
        marklogic_txn,
        MarkLogicURIs.kb_data(),
    )
    if content is None:
        return None
    result = KBData()
    for column in columns:
        if column == "slug":
            # Slug is fetched from the config
            value = _deserialize("config", content["config"]).slug
        elif column in ("config", "shards"):
            value = _deserialize(column, content[column])
        setattr(result, column, value)
    return result


async def iter(txn: Transaction, *, slug_prefix: str = "") -> AsyncIterator[tuple[str, str]]:
    # TODO(Marklogic): Can't we already filter in optic those registry items that are deleted or don't have a slug?
    query = f"cts.collectionQuery({json.dumps(MarkLogicCollections.KB_REGISTRY_ITEM)})"
    if slug_prefix:
        query += f", cts.jsonPropertyValueQuery('slug', {json.dumps(slug_prefix + '*')}, ['wildcarded'])"
    javascript = f"cts.uris('', ['document', 'item-order'], cts.andQuery([{query}]))"
    _, marklogic_txn = _driver_txn(txn, system_scope=True)
    uris = await documents.evaluate(marklogic_txn, javascript)
    for uri in uris:
        kbid = MarkLogicURIs.kbid_from_registry(uri)
        registry = await _get_registry(txn, kbid=kbid)
        if registry is None:
            continue
        slug = registry.slug
        if slug is not None and registry.deleted_at is None:
            yield kbid, slug


@observer.wrap({"type": "kb", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str) -> bool:
    _, marklogic_txn = _driver_txn(txn, system_scope=True)
    content = await documents.read(marklogic_txn, MarkLogicURIs.kb_registry(kbid))
    return content is not None and content.get("slug") is not None and content.get("deleted_at") is None


async def exists_by_id(driver: MarkLogicDriver, *, kbid: str) -> bool:
    async with driver.transaction(read_only=True, system=True) as txn:
        return await exists(txn, kbid=kbid)


@observer.wrap({"type": "kb", "op": "get_kbid"})
async def get_kbid(txn: Transaction, *, slug: str) -> str | None:
    return await _get_kbid_from_slug(txn, slug=slug)


async def _get_kbid_from_slug(txn: Transaction, *, slug: str) -> str | None:
    # TODO(Marklogic): Can't we already filter in optic those registry items that are deleted or don't have a slug?
    _, marklogic_txn = _driver_txn(txn, system_scope=True)
    javascript = (
        "cts.uris('', ['document'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.KB_REGISTRY_ITEM)}), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.SLUG)}, '=', {json.dumps(slug)})]))"
    )
    uris = await documents.evaluate(marklogic_txn, javascript)
    active_kbids = []
    for uri in uris:
        kbid = MarkLogicURIs.kbid_from_registry(uri)
        registry = await _get_registry(txn, kbid=kbid)
        if registry is not None and registry.deleted_at is None:
            active_kbids.append(kbid)
    if len(active_kbids) == 0:
        return None
    elif len(active_kbids) > 1:
        raise RuntimeError(f"Multiple KBs found for slug {slug}")
    return active_kbids[0]


@observer.wrap({"type": "kb", "op": "set_slug"})
async def set_slug(txn: Transaction, *, slug: str, kbid: str) -> None:
    """
    This is the first step in creating or updating the slug for a knowledge box.
    It ensures that the slug is unique and that the corresponding knowledge box database exists.
    """
    existing = await _get_kbid_from_slug(txn, slug=slug)
    if existing is not None and existing != kbid:
        raise KnowledgeBoxConflict()
    await _set_registry(txn, kbid=kbid, slug=slug)


@observer.wrap({"type": "kb", "op": "delete"})
async def delete(txn: Transaction, *, kbid: str) -> None:
    driver, marklogic_txn = _driver_txn(txn, system_scope=True, ensure_writes=True)
    await driver.delete_kb_database(kbid)
    await documents.delete(marklogic_txn, MarkLogicURIs.kb_registry(kbid))


@observer.wrap({"type": "kb", "op": "registry_delete"})
async def registry_delete(txn: Transaction, *, kbid: str) -> None:
    _, marklogic_txn = _driver_txn(txn, system_scope=True)
    await documents.delete(marklogic_txn, MarkLogicURIs.kb_registry(kbid))


@observer.wrap({"type": "kb", "op": "soft_delete"})
async def soft_delete(txn: Transaction, *, kbid: str) -> None:
    # TODO(Marklogic): Implement patch behaviour with logic to avoid read-modify-write race conditions.
    registry = await _get_registry(txn, kbid=kbid)
    if registry is None:
        return
    await _set_registry(txn, kbid=kbid, slug="", deleted_at=datetime.now(timezone.utc).isoformat())


@observer.wrap({"type": "kb", "op": "get_config"})
async def get_config(txn: Transaction, *, kbid: str, for_update: bool = False):
    data = await _get_data(txn, kbid=kbid, columns=("config",), for_update=for_update)
    return data.config if isinstance(data, KBData) else None


@observer.wrap({"type": "kb", "op": "get_shards"})
async def get_shards(txn: Transaction, *, kbid: str, for_update: bool = False):
    data = await _get_data(txn, kbid=kbid, columns=("shards",), for_update=for_update)
    return data.shards if isinstance(data, KBData) else None


async def get_model_metadata(txn: Transaction, *, kbid: str) -> knowledgebox_pb2.SemanticModelMetadata:
    shards_obj = await get_shards(txn, kbid=kbid)
    if shards_obj is None:
        raise KnowledgeBoxNotFound(kbid)
    if shards_obj.HasField("model"):
        return shards_obj.model
    return knowledgebox_pb2.SemanticModelMetadata(similarity_function=shards_obj.similarity)


async def get_matryoshka_vector_dimension(
    txn: Transaction, *, kbid: str, vectorset_id: str | None = None
) -> int | None:
    from . import vectorsets

    async for _, vs in vectorsets.iter(txn, kbid=kbid):
        dimension = vs.vectorset_index_config.vector_dimension
        if vs.matryoshka_dimensions and dimension:
            return dimension if dimension in vs.matryoshka_dimensions else None
        return None
    model = await get_model_metadata(txn, kbid=kbid)
    configured_dimension = model.vector_dimension if model.HasField("vector_dimension") else None
    return (
        configured_dimension
        if configured_dimension is not None and configured_dimension in model.matryoshka_dimensions
        else None
    )
