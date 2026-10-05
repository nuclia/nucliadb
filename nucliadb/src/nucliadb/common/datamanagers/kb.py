"""MarkLogic datamanager for KnowledgeBoxes."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Final, Literal, TypeAlias, cast

from nucliadb.common.datamanagers import marklogic_documents as documents
from nucliadb.common.datamanagers.exceptions import KnowledgeBoxConflict, KnowledgeBoxNotFound
from nucliadb.common.datamanagers.utils import UNSET, _UnsetType, observer
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.maindb.utils import get_driver
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


def _driver_txn(
    txn: Transaction, ensure_writes: bool = False
) -> tuple[MarkLogicDriver, MarkLogicTransaction]:
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver) or not isinstance(txn, MarkLogicTransaction):
        raise TypeError("KnowledgeBox datamanager requires MarkLogicDriver")
    if ensure_writes and txn.read_only:
        raise RuntimeError("Cannot write in read only transaction")
    return driver, txn


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
) -> None:
    driver, marklogic_txn = _driver_txn(txn, ensure_writes=True)
    await documents.write(
        marklogic_txn,
        driver.system_database,
        _registry_uri(kbid),
        MarkLogicCollections.KB_REGISTRY_ITEM,
        {"kbid": kbid, "slug": slug},
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
    # TODO(Marklogic): Look into optimizing read-modify-write for KB data (optic DSL)
    if config is UNSET and shards is UNSET:
        return
    driver, marklogic_txn = _driver_txn(txn, ensure_writes=True)
    content = await documents.read(
        marklogic_txn,
        driver.kb_database(kbid),
        _kb_uri(),
    )
    if content is None:
        # Initialize content with the kbid if it doesn't exist
        content = {"kbid": kbid}
    for name, value in (("config", config), ("shards", shards)):
        if value is not UNSET:
            content[name] = _serialize(value)
    await documents.write(
        marklogic_txn,
        driver.kb_database(kbid),
        _kb_uri(),
        MarkLogicCollections.KNOWLEDGEBOXES,
        content,
    )


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
    driver, marklogic_txn = _driver_txn(txn)
    content = await documents.read(
        marklogic_txn,
        driver.kb_database(kbid),
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
    driver, marklogic_txn = _driver_txn(txn)
    query = f"cts.collectionQuery({json.dumps(MarkLogicCollections.KB_REGISTRY_ITEM)})"
    if slug_prefix:
        query += f", cts.jsonPropertyValueQuery('slug', {json.dumps(slug_prefix + '*')}, ['wildcarded'])"
    javascript = f"cts.uris('', ['document', 'item-order'], cts.andQuery([{query}]))"
    uris = await documents.evaluate(marklogic_txn, driver.system_database, javascript)
    for uri in uris:
        kbid = _kbid_from_registry_uri(uri)
        content = await documents.read(marklogic_txn, driver.system_database, _registry_uri(kbid))
        if content is None:
            continue
        slug = content.get("slug")
        if slug is not None:
            yield kbid, slug


@observer.wrap({"type": "kb", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str) -> bool:
    driver, marklogic_txn = _driver_txn(txn)
    content = await documents.read(marklogic_txn, driver.system_database, _registry_uri(kbid))
    return content is not None and content.get("slug") is not None


@observer.wrap({"type": "kb", "op": "get_kbid"})
async def get_kbid(txn: Transaction, *, slug: str) -> str | None:
    return await _get_kbid_from_slug(txn, slug=slug)


async def _get_kbid_from_slug(txn: Transaction, *, slug: str) -> str | None:
    driver, _ = _driver_txn(txn)
    javascript = (
        "cts.uris('', ['document'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.KB_REGISTRY_ITEM)}), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.SLUG)}, '=', {json.dumps(slug)})]))"
    )
    uris = await documents.evaluate(txn, driver.system_database, javascript)
    if len(uris) == 0:
        return None
    elif len(uris) > 1:
        raise RuntimeError(f"Multiple KBs found for slug {slug}")
    return _kbid_from_registry_uri(uris[0])


@observer.wrap({"type": "kb", "op": "set_slug"})
async def set_slug(txn: Transaction, *, slug: str, kbid: str) -> None:
    driver, _ = _driver_txn(txn, ensure_writes=True)
    existing = await _get_kbid_from_slug(txn, slug=slug)
    if not existing:
        # If the slug does not exist, make sure to create the database
        await driver.ensure_kb_database(kbid)
    elif existing != kbid:
        raise KnowledgeBoxConflict()
    await _set_registry(txn, kbid=kbid, slug=slug)


@observer.wrap({"type": "kb", "op": "delete"})
async def delete(txn: Transaction, *, kbid: str) -> None:
    driver, marklogic_txn = _driver_txn(txn, ensure_writes=True)
    await driver.delete_kb_database(kbid)
    await documents.delete(marklogic_txn, driver.system_database, _registry_uri(kbid))


@observer.wrap({"type": "kb", "op": "soft_delete"})
async def soft_delete(txn: Transaction, *, kbid: str) -> None:
    # TODO(Marklogic): implement soft deletion + purge
    return


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


async def get_external_index_provider_metadata(txn: Transaction, *, kbid: str):
    config = await get_config(txn, kbid=kbid)
    return config.external_index_provider if config is not None else None


async def set_external_index_provider_metadata(txn: Transaction, *, kbid: str, metadata):
    config = await get_config(txn, kbid=kbid)
    if config is None:
        raise KnowledgeBoxNotFound(kbid)
    config.external_index_provider.CopyFrom(metadata)
    await _set_data(txn, kbid=kbid, config=config)
