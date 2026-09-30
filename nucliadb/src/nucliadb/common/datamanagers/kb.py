"""MarkLogic datamanager for KnowledgeBoxes."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import Final, Literal, TypeAlias, cast

from marklogic.documents import Document  # type: ignore[import-untyped]

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxConflict, KnowledgeBoxNotFound
from nucliadb.common.datamanagers.utils import UNSET, _UnsetType, observer
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb_protos import knowledgebox_pb2, writer_pb2

logger = logging.getLogger(__name__)

KBColumn: TypeAlias = Literal["slug", "config", "shards", "deleted_at"]
UNSET_STR: Final[str | None] = cast(str | None, UNSET)
UNSET_CONFIG: Final[knowledgebox_pb2.KnowledgeBoxConfig | None] = cast(
    knowledgebox_pb2.KnowledgeBoxConfig | None, UNSET
)
UNSET_SHARDS: Final[writer_pb2.Shards | None] = cast(writer_pb2.Shards | None, UNSET)
UNSET_DELETED_AT: Final[datetime | None] = cast(datetime | None, UNSET)


@dataclass(slots=True)
class KBData:
    slug: str | None = UNSET_STR
    config: knowledgebox_pb2.KnowledgeBoxConfig | None = UNSET_CONFIG
    shards: writer_pb2.Shards | None = UNSET_SHARDS
    deleted_at: datetime | None = UNSET_DELETED_AT


def _driver_txn(txn: Transaction) -> tuple[MarkLogicDriver, MarkLogicTransaction]:
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver) or not isinstance(txn, MarkLogicTransaction):
        raise TypeError("KnowledgeBox datamanager requires MarkLogicDriver")
    return driver, txn


def _uri(kbid: str) -> str:
    return f"knowledgeboxes/{kbid}/config.json"


def _tx_params(driver: MarkLogicDriver, txn: MarkLogicTransaction) -> dict[str, str]:
    params = {"database": driver.database}
    if txn.transaction is not None:
        params["txid"] = txn.transaction.id
    return params


def _encode(value: bytes | None) -> str | None:
    return base64.b64encode(value).decode("ascii") if value is not None else None


def _decode(value: str | None) -> bytes | None:
    return base64.b64decode(value) if value is not None else None


def _content(document: Document | None) -> dict | None:
    if document is None:
        return None
    if not isinstance(document.content, dict):
        raise RuntimeError(f"Invalid KnowledgeBox document: {document.uri}")
    return dict(document.content)


async def _get(driver: MarkLogicDriver, txn: MarkLogicTransaction, kbid: str) -> Document | None:
    result = await asyncio.to_thread(
        driver.client.documents.read,
        _uri(kbid),
        tx=txn.transaction,
        params=_tx_params(driver, txn),
    )
    return result[0] if isinstance(result, list) and result else None


async def _write(driver: MarkLogicDriver, txn: MarkLogicTransaction, kbid: str, content: dict) -> None:
    response = await asyncio.to_thread(
        driver.client.documents.write,
        Document(
            uri=_uri(kbid),
            content=content,
            collections=[MarkLogicCollections.KNOWLEDGEBOXES],
            content_type="application/json",
        ),
        tx=txn.transaction,
        params={"database": driver.database},
    )
    if not response.ok:
        raise RuntimeError(f"Failed to write KnowledgeBox: {response.status_code} {response.text}")


async def _delete(driver: MarkLogicDriver, txn: MarkLogicTransaction, kbid: str) -> None:
    response = await asyncio.to_thread(
        driver.client.delete,
        "/v1/documents",
        params={"uri": _uri(kbid), **_tx_params(driver, txn)},
    )
    if not response.ok:
        raise RuntimeError(f"Failed to delete KnowledgeBox: {response.status_code} {response.text}")


def _serialize(value: knowledgebox_pb2.KnowledgeBoxConfig | writer_pb2.Shards | None | _UnsetType):
    if value is UNSET:
        return UNSET
    if value is None:
        return None
    if not isinstance(value, (knowledgebox_pb2.KnowledgeBoxConfig, writer_pb2.Shards)):
        raise ValueError(f"Unsupported KnowledgeBox column value: {type(value)}")
    return _encode(value.SerializeToString())


def _deserialize(column: KBColumn, value):
    if value is None:
        return None
    raw = _decode(value)
    if raw is None:
        return None
    if column == "config":
        config = knowledgebox_pb2.KnowledgeBoxConfig()
        config.ParseFromString(raw)
        return config
    elif column == "shards":
        shards = writer_pb2.Shards()
        shards.ParseFromString(raw)
        return shards
    return value


@observer.wrap({"type": "kb", "op": "set"})
async def set(
    txn: Transaction,
    *,
    kbid: str,
    config: knowledgebox_pb2.KnowledgeBoxConfig | None | _UnsetType = UNSET,
    shards: writer_pb2.Shards | None | _UnsetType = UNSET,
) -> None:
    driver, marklogic_txn = _driver_txn(txn)
    content = _content(await _get(driver, marklogic_txn, kbid)) or {}
    content["kbid"] = kbid
    for name, value in (("config", config), ("shards", shards)):
        if value is not UNSET:
            content[name] = _serialize(value)
    if config is not UNSET or shards is not UNSET:
        await _write(driver, marklogic_txn, kbid, content)


@observer.wrap({"type": "kb", "op": "get"})
async def get(
    txn: Transaction,
    *,
    kbid: str,
    columns: tuple[KBColumn, ...],
    for_update: bool = False,
) -> KBData | None:
    if not columns:
        raise ValueError("At least one KB column must be requested")
    driver, marklogic_txn = _driver_txn(txn)
    document = await _get(driver, marklogic_txn, kbid)
    if document is None:
        return None
    content = _content(document)
    if content is None:
        return None
    result = KBData()
    for column in columns:
        value = content.get(column)
        if column == "deleted_at" and value is not None:
            value = datetime.fromisoformat(value)
        elif column in ("config", "shards"):
            value = _deserialize(column, value)
        setattr(result, column, value)
    return result


async def iter(txn: Transaction, *, slug_prefix: str = "") -> AsyncIterator[tuple[str, str]]:
    driver, marklogic_txn = _driver_txn(txn)
    query = f"cts.collectionQuery({json.dumps(MarkLogicCollections.KNOWLEDGEBOXES)})"
    if slug_prefix:
        query += f", cts.jsonPropertyValueQuery('slug', {json.dumps(slug_prefix + '*')}, ['wildcarded'])"
    javascript = f"cts.uris('', ['document', 'item-order'], cts.andQuery([{query}]))"
    uris = (
        await asyncio.to_thread(
            driver.client.eval,
            javascript=javascript,
            params=_tx_params(driver, marklogic_txn),
        )
        or []
    )
    for uri in uris:
        kbid = str(uri).removeprefix("knowledgeboxes/").removesuffix("/config.json")
        document = await _get(driver, marklogic_txn, kbid)
        content = _content(document)
        if content is None:
            continue
        slug = content.get("slug")
        if slug is not None:
            yield kbid, slug


@observer.wrap({"type": "kb", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str) -> bool:
    driver, marklogic_txn = _driver_txn(txn)
    document = await _get(driver, marklogic_txn, kbid)
    if document is None:
        return False
    content = _content(document)
    return content is not None and content.get("slug") is not None and content.get("deleted_at") is None


@observer.wrap({"type": "kb", "op": "get_kbid"})
async def get_kbid(txn: Transaction, *, slug: str) -> str | None:
    driver, marklogic_txn = _driver_txn(txn)
    javascript = (
        "cts.uris('', ['document'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.KNOWLEDGEBOXES)}), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.KNOWLEDGEBOX_SLUG)}, '=', {json.dumps(slug)})]))"
    )
    uris = (
        await asyncio.to_thread(
            driver.client.eval,
            javascript=javascript,
            params=_tx_params(driver, marklogic_txn),
        )
        or []
    )
    return str(uris[0]).removeprefix("knowledgeboxes/").removesuffix("/config.json") if uris else None


@observer.wrap({"type": "kb", "op": "set_slug"})
async def set_slug(txn: Transaction, *, slug: str, kbid: str) -> None:
    driver, marklogic_txn = _driver_txn(txn)
    document = await _get(driver, marklogic_txn, kbid)
    content = _content(document)
    if content is None:
        content = {}
    same_slug = content.get("slug") == slug
    has_kbid = content.get("kbid") == kbid
    content["kbid"] = kbid
    if same_slug and has_kbid:
        return

    existing = await get_kbid(txn, slug=slug)
    if existing is not None and existing != kbid:
        raise KnowledgeBoxConflict()
    content["slug"] = slug
    await _write(driver, marklogic_txn, kbid, content)


@observer.wrap({"type": "kb", "op": "delete"})
async def delete(txn: Transaction, *, kbid: str) -> None:
    driver, marklogic_txn = _driver_txn(txn)
    await _delete(driver, marklogic_txn, kbid)


@observer.wrap({"type": "kb", "op": "soft_delete"})
async def soft_delete(txn: Transaction, *, kbid: str) -> None:
    driver, marklogic_txn = _driver_txn(txn)
    document = await _get(driver, marklogic_txn, kbid)
    if document is None:
        return
    content = _content(document)
    if content is None:
        return
    content["slug"] = None
    content["deleted_at"] = datetime.now().isoformat()
    await _write(driver, marklogic_txn, kbid, content)


@observer.wrap({"type": "kb", "op": "get_config"})
async def get_config(txn: Transaction, *, kbid: str, for_update: bool = False):
    data = await get(txn, kbid=kbid, columns=("config",), for_update=for_update)
    return data.config if data is not None else None


@observer.wrap({"type": "kb", "op": "get_shards"})
async def get_shards(txn: Transaction, *, kbid: str, for_update: bool = False):
    data = await get(txn, kbid=kbid, columns=("shards",), for_update=for_update)
    return data.shards if data is not None else None


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
    await set(txn, kbid=kbid, config=config)
