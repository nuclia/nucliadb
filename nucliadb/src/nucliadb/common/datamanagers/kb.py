"""MarkLogic datamanager for KnowledgeBoxes."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final, Literal, TypeAlias, cast

from nucliadb.common.datamanagers import marklogic_documents as documents
from nucliadb.common.datamanagers.exceptions import KnowledgeBoxConflict, KnowledgeBoxNotFound
from nucliadb.common.datamanagers.marklogic_documents import driver_txn
from nucliadb.common.datamanagers.utils import UNSET, _UnsetType, observer
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.marklogic.document_update import DocumentUpdate
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


def _registry_uri(kbid: str) -> str:
    return f"/kb/{kbid}/registry.json"


def _kbid_from_registry_uri(uri: str) -> str:
    # Assumes the URI is of the form "/kb/{kbid}/registry.json"
    parts = uri.strip("/").split("/")
    if len(parts) != 3 or parts[0] != "kb" or parts[2] != "registry.json":
        raise ValueError(f"Invalid registry URI: {uri}")
    return parts[1]


def _kb_uri() -> str:
    return "/kb_data.json"


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
    documents.require_system_scope(txn)
    _, marklogic_txn = driver_txn(txn, ensure_writes=True)
    await documents.write(
        marklogic_txn,
        _registry_uri(kbid),
        MarkLogicCollections.KB_REGISTRY_ITEM,
        {"kbid": kbid, "slug": slug, "deleted_at": deleted_at},
    )


async def _get_registry(
    txn: Transaction,
    *,
    kbid: str,
) -> KBRegistry | None:
    documents.require_system_scope(txn)
    content = await documents.read(
        txn,
        _registry_uri(kbid),
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
    documents.require_kb_scope(txn, kbid)
    update = DocumentUpdate(
        uri=_kb_uri(), collection=MarkLogicCollections.KNOWLEDGEBOXES, defaults={"kbid": kbid}
    )
    for name, value in (("config", config), ("shards", shards)):
        if value is not UNSET:
            update[name] = _serialize(value)
    await documents.update_document(txn, update)


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
    documents.require_kb_scope(txn, kbid)
    if not columns:
        raise ValueError("At least one KB column must be requested")
    content = await documents.read(
        txn,
        _kb_uri(),
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
    documents.require_system_scope(txn)
    query = f"cts.collectionQuery({json.dumps(MarkLogicCollections.KB_REGISTRY_ITEM)})"
    if slug_prefix:
        query += f", cts.jsonPropertyValueQuery('slug', {json.dumps(slug_prefix + '*')}, ['wildcarded'])"
    javascript = f"cts.uris('', ['document', 'item-order'], cts.andQuery([{query}]))"
    uris = await documents.evaluate(txn, javascript)
    for uri in uris:
        kbid = _kbid_from_registry_uri(uri)
        registry = await _get_registry(txn, kbid=kbid)
        if registry is None:
            continue
        slug = registry.slug
        if slug is not None and registry.deleted_at is None:
            yield kbid, slug


@observer.wrap({"type": "kb", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str) -> bool:
    documents.require_system_scope(txn)
    content = await documents.read(txn, _registry_uri(kbid))
    return content is not None and content.get("slug") is not None and content.get("deleted_at") is None


@observer.wrap({"type": "kb", "op": "get_kbid"})
async def get_kbid(txn: Transaction, *, slug: str) -> str | None:
    return await _get_kbid_from_slug(txn, slug=slug)


async def _get_kbid_from_slug(txn: Transaction, *, slug: str) -> str | None:
    # TODO(Marklogic): Can't we already filter in optic those registry items that are deleted or don't have a slug?
    documents.require_system_scope(txn)
    javascript = (
        "cts.uris('', ['document'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.KB_REGISTRY_ITEM)}), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.SLUG)}, '=', {json.dumps(slug)})]))"
    )
    uris = await documents.evaluate(txn, javascript)
    active_kbids = []
    for uri in uris:
        kbid = _kbid_from_registry_uri(uri)
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
    documents.require_system_scope(txn)
    driver, _ = driver_txn(txn, ensure_writes=True)
    existing = await _get_kbid_from_slug(txn, slug=slug)
    if not existing:
        # If the slug does not exist, make sure to create the database
        await driver.ensure_kb_database(kbid)
    elif existing != kbid:
        raise KnowledgeBoxConflict()
    await _set_registry(txn, kbid=kbid, slug=slug)


@observer.wrap({"type": "kb", "op": "delete"})
async def delete(txn: Transaction, *, kbid: str) -> None:
    documents.require_system_scope(txn)
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    await driver.delete_kb_database(kbid)
    await documents.delete(marklogic_txn, _registry_uri(kbid))


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
