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
"""MarkLogic datamanager for resource fields and their processed documents."""

import base64
import json
from typing import Any, Sequence

from google.protobuf.message import Message
from pydantic import BaseModel

from nucliadb.common.datamanagers.utils import observer
from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.marklogic import MarkLogicTransaction
from nucliadb.common.marklogic import documents, vector_views
from nucliadb.common.marklogic.client import Document
from nucliadb.common.marklogic.document_deletion import DocumentDeletion
from nucliadb.common.marklogic.document_update import DocumentUpdate
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.common.models_utils import from_proto
from nucliadb_protos import resources_pb2 as rpb2
from nucliadb_protos import writer_pb2 as wpb2


class FieldContent(BaseModel):
    resource_id: str
    field_id: str
    field_type: str
    value: dict[str, Any] | None = None
    md5: str | None = None
    status: dict[str, Any] | None = None


class FieldProcessedContent(BaseModel):
    resource_id: str
    field_id: str
    field_type: str
    extracted_text: str
    facets: list[str]
    # Copies of the resource properties, see resources.PROCESSED_FIELD_COPIES
    resource_facets: list[str] = []
    security: dict[str, Any] | None = None


class ChunkContent(BaseModel):
    resource_id: str
    field_id: str
    field_type: str
    chunk_id: str
    text: str
    facets: list[str]
    data: dict[str, Any]


class ChunkEmbeddingContent(BaseModel):
    resource_id: str
    field_id: str
    field_type: str
    chunk_id: str
    model_id: str
    chunk_embedding_index: int
    vector: list[float]
    data: dict[str, Any]


_FIELD_VALUE_RAW_KEY = "_raw_bytes_base64"


def _marklogic_txn(
    txn: Transaction,
    *,
    kbid: str | None = None,
    ensure_writes: bool = False,
) -> MarkLogicTransaction:
    documents.driver_txn(txn, ensure_writes=ensure_writes)
    if txn.kbid is None:
        raise ValueError("Field operations require a KnowledgeBox-scoped transaction")
    if kbid is not None and txn.kbid != kbid:
        raise ValueError("Transaction KnowledgeBox does not match the requested KnowledgeBox")
    return txn


def _field_update(
    *,
    rid: str,
    field_type: str,
    field_id: str,
    **values: Any,
) -> DocumentUpdate:
    return DocumentUpdate(
        uri=MarkLogicURIs.field(rid, field_type, field_id),
        collection=MarkLogicCollections.FIELDS,
        defaults={"resource_id": rid, "field_id": field_id, "field_type": field_type},
    ).set(**values)


def _value_to_raw_bytes(value: dict[str, Any] | None, field_type: str) -> bytes | None:
    if value is None:
        return None
    raw_value = value.get(_FIELD_VALUE_RAW_KEY)
    if isinstance(raw_value, str):
        return base64.b64decode(raw_value)
    pb_types: dict[str, type[Message]] = {
        "t": rpb2.FieldText,
        "f": rpb2.FieldFile,
        "u": rpb2.FieldLink,
        "k": rpb2.FieldKeyValue,
        "c": rpb2.Conversation,
    }
    pb_type = pb_types.get(field_type)
    if pb_type is None:
        raise ValueError(f"Field type {field_type!r} has no protobuf value type")
    return documents.from_json(value, pb_type).SerializeToString()


@observer.wrap({"type": "field", "op": "create_document"})
async def create_document(txn: Transaction, data: FieldContent) -> None:
    marklogic_txn = _marklogic_txn(txn, ensure_writes=True)
    await documents.write(
        marklogic_txn,
        uri=MarkLogicURIs.field(data.resource_id, data.field_type, data.field_id),
        collection=MarkLogicCollections.FIELDS,
        content=data.model_dump(mode="json"),
    )


@observer.wrap({"type": "field", "op": "list_resource_documents"})
async def list_resource_documents(
    txn: Transaction,
    *,
    resource_id: str,
) -> list[tuple[wpb2.FieldType.ValueType, str]]:
    marklogic_txn = _marklogic_txn(txn)
    directory = MarkLogicURIs.fields_directory(resource_id)
    javascript = (
        "cts.uris('', ['document', 'item-order'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.FIELDS)}), "
        f"cts.directoryQuery({json.dumps(directory)}, 'infinity')]))"
    )
    uris = await documents.evaluate(marklogic_txn, javascript)
    fields = []
    for uri in uris:
        field_type, field_id = str(uri).removeprefix(directory).removesuffix("/data.json").split("/", 1)
        fields.append((FIELD_TYPE_STR_TO_PB[field_type], field_id))
    return fields


@observer.wrap({"type": "field", "op": "get_document"})
async def get_document(
    txn: Transaction,
    *,
    resource_id: str,
    field_id: str,
    field_type: str,
) -> FieldContent | None:
    marklogic_txn = _marklogic_txn(txn)
    content = await documents.read(marklogic_txn, MarkLogicURIs.field(resource_id, field_type, field_id))
    return FieldContent.model_validate(content) if content is not None else None


@observer.wrap({"type": "field", "op": "create_processed_documents"})
async def create_processed_documents(
    txn: Transaction,
    *,
    field: FieldProcessedContent,
    chunks: list[ChunkContent],
    embeddings: list[ChunkEmbeddingContent],
    skip_field_texts: bool = False,
    skip_chunks: bool = False,
) -> None:
    marklogic_txn = _marklogic_txn(txn, ensure_writes=True)
    if not skip_field_texts:
        await documents.write(
            marklogic_txn,
            uri=MarkLogicURIs.field_processed(field.resource_id, field.field_id, field.field_type),
            collection=MarkLogicCollections.FIELDS_PROCESSED,
            content=field.model_dump(mode="json"),
        )
    if not skip_chunks and chunks:
        await documents.write_many(
            marklogic_txn,
            documents_to_write=[
                Document(
                    uri=MarkLogicURIs.chunk(
                        chunk.resource_id, chunk.field_type, chunk.field_id, chunk.chunk_id
                    ),
                    collections=[MarkLogicCollections.CHUNKS],
                    content=chunk.model_dump(mode="json"),
                    content_type="application/json",
                )
                for chunk in chunks
            ],
        )
    if embeddings:
        for dimension in {len(embedding.vector) for embedding in embeddings}:
            await vector_views.ensure_view(marklogic_txn.driver, marklogic_txn.database, dimension)
        await documents.write_many(
            marklogic_txn,
            documents_to_write=[
                Document(
                    uri=MarkLogicURIs.embedding(
                        embedding.resource_id,
                        embedding.field_type,
                        embedding.field_id,
                        embedding.model_id,
                        embedding.chunk_id,
                    ),
                    collections=[
                        MarkLogicCollections.EMBEDDINGS,
                        vector_views.collection(len(embedding.vector)),
                    ],
                    content=embedding.model_dump(mode="json"),
                    content_type="application/json",
                )
                for embedding in embeddings
            ],
        )


@observer.wrap({"type": "field", "op": "delete_processed_documents"})
async def delete_processed_documents(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str | None = None,
    field_id: str | None = None,
    field_texts: bool = True,
    chunks: bool = True,
    embeddings: bool = True,
    embedding_models: Sequence[str] | None = None,
) -> None:
    """
    Delete processed texts, chunks and embeddings, keeping field values. Scoped to a single field when
    `field_type` and `field_id` are given, otherwise to all the resource fields. `embedding_models=None`
    deletes the embeddings of all models.
    """
    if (field_type is None) != (field_id is None):
        raise ValueError("field_type and field_id must be provided together")
    marklogic_txn = _marklogic_txn(txn, kbid=kbid, ensure_writes=True)
    deletion = DocumentDeletion()
    if field_type is None or field_id is None:
        if embedding_models is not None:
            raise ValueError("embedding_models requires a field")
        collections = [
            collection
            for collection, selected in (
                (MarkLogicCollections.FIELDS_PROCESSED, field_texts),
                (MarkLogicCollections.CHUNKS, chunks),
                (MarkLogicCollections.EMBEDDINGS, embeddings),
            )
            if selected
        ]
        if collections:
            deletion.directory(MarkLogicURIs.fields_directory(rid), collections)
    else:
        if field_texts:
            deletion.uri(MarkLogicURIs.field_processed(rid, field_id, field_type))
        if chunks:
            deletion.directory(
                MarkLogicURIs.chunks_directory(rid, field_type, field_id),
                (MarkLogicCollections.CHUNKS,),
            )
        if embeddings:
            for model_id in embedding_models if embedding_models is not None else (None,):
                deletion.directory(
                    MarkLogicURIs.embeddings_directory(rid, field_type, field_id, model_id),
                    (MarkLogicCollections.EMBEDDINGS,),
                )
    if deletion:
        await documents.delete_documents(marklogic_txn, deletion)


@observer.wrap({"type": "field", "op": "set_status"})
async def set_status(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    status: wpb2.FieldStatus,
) -> None:
    marklogic_txn = _marklogic_txn(txn, kbid=kbid, ensure_writes=True)
    await documents.update_document(
        marklogic_txn,
        _field_update(
            rid=rid,
            field_type=field_type,
            field_id=field_id,
            status=documents.to_json(status),
        ),
    )


@observer.wrap({"type": "field", "op": "set"})
async def set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    value: Message | bytes,
    status: wpb2.FieldStatus | None = None,
    md5: str | None = None,
) -> None:
    if isinstance(value, Message):
        value_json = documents.to_json(value)
    else:
        value_json = {_FIELD_VALUE_RAW_KEY: base64.b64encode(value).decode("ascii")}

    marklogic_txn = _marklogic_txn(txn, kbid=kbid, ensure_writes=True)
    await documents.update_document(
        marklogic_txn,
        _field_update(
            rid=rid,
            field_type=field_type,
            field_id=field_id,
            value=value_json,
            status=documents.to_json(status) if status is not None else None,
            md5=md5,
        ),
    )


@observer.wrap({"type": "field", "op": "delete"})
async def delete(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> None:
    marklogic_txn = _marklogic_txn(txn, kbid=kbid, ensure_writes=True)
    deletion = DocumentDeletion().uri(MarkLogicURIs.field(rid, field_type, field_id))
    deletion.directory(
        MarkLogicURIs.field_directory(rid, field_type, field_id),
        (
            MarkLogicCollections.FIELDS_PROCESSED,
            MarkLogicCollections.CONVERSATIONS,
            MarkLogicCollections.CHUNKS,
            MarkLogicCollections.EMBEDDINGS,
        ),
    )
    await documents.delete_documents(marklogic_txn, deletion)


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


@observer.wrap({"type": "field", "op": "get_raw"})
async def get_raw(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> bytes | None:
    _marklogic_txn(txn, kbid=kbid)
    content = await get_document(txn, resource_id=rid, field_id=field_id, field_type=field_type)
    if content is None:
        return None
    return _value_to_raw_bytes(content.value, field_type)


@observer.wrap({"type": "field", "op": "get_status"})
async def get_status(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> wpb2.FieldStatus | None:
    _marklogic_txn(txn, kbid=kbid)
    content = await get_document(txn, resource_id=rid, field_id=field_id, field_type=field_type)
    if content is None or content.status is None:
        return None
    return documents.from_json(content.status, wpb2.FieldStatus)


@observer.wrap({"type": "field", "op": "get_statuses"})
async def get_statuses(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    fields: Sequence[rpb2.FieldID],
) -> list[wpb2.FieldStatus]:
    if not fields:
        return []
    statuses = []
    for field in fields:
        status = await get_status(
            txn,
            kbid=kbid,
            rid=rid,
            field_type=_to_abbr(field.field_type),
            field_id=field.field,
        )
        statuses.append(status if status is not None else wpb2.FieldStatus())
    return statuses


def _to_abbr(field_type: rpb2.FieldType.ValueType) -> str:
    """Convert a FieldType enum to its single-character abbreviation."""
    return from_proto.field_type_name(field_type).abbreviation()


@observer.wrap({"type": "field", "op": "get_all_field_ids"})
async def get_all_field_ids(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
) -> rpb2.AllFieldIDs:
    _marklogic_txn(txn, kbid=kbid)
    pb = rpb2.AllFieldIDs()
    for field_type, field_id in await list_resource_documents(txn, resource_id=rid):
        if field_type == rpb2.FieldType.GENERIC and field_id in ("title", "summary"):
            continue
        field = pb.fields.add()
        field.field_type = field_type
        field.field = field_id
    return pb


@observer.wrap({"type": "field", "op": "exists"})
async def exists(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: rpb2.FieldID,
) -> bool:
    _marklogic_txn(txn, kbid=kbid)
    return (
        await get_document(
            txn,
            resource_id=rid,
            field_id=field_id.field,
            field_type=from_proto.field_type_name(field_id.field_type).abbreviation(),
        )
        is not None
    )


@observer.wrap({"type": "field", "op": "exists_md5"})
async def exists_md5(
    txn: Transaction,
    *,
    kbid: str,
    md5: str,
    field_type: str,
) -> bool:
    marklogic_txn = _marklogic_txn(txn, kbid=kbid)
    javascript = (
        "cts.uris('', ['document'], cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.FIELDS)}), "
        f"cts.pathRangeQuery('/md5', '=', {json.dumps(md5)}), "
        f"cts.jsonPropertyValueQuery('field_type', {json.dumps(field_type)})]))"
    )
    return bool(await documents.evaluate(marklogic_txn, javascript))


@observer.wrap({"type": "field", "op": "set_md5"})
async def set_md5(
    txn: Transaction, *, kbid: str, md5: str, rid: str, field_id: str, field_type: str
) -> None:
    marklogic_txn = _marklogic_txn(txn, kbid=kbid, ensure_writes=True)
    update = _field_update(
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        md5=md5,
    )
    await documents.update_document(marklogic_txn, update)
