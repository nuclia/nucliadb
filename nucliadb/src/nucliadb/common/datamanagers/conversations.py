"""MarkLogic datamanager for conversation field metadata, pages, and splits."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from nucliadb.common.datamanagers.utils import observer
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.marklogic import MarkLogicTransaction
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.document_deletion import DocumentDeletion
from nucliadb.common.marklogic.document_update import DocumentUpdate
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import FieldConversation, SplitsMetadata
from nucliadb_protos.writer_pb2 import FieldStatus

_SPLITS_METADATA_PAGE = 0


class ConversationPageContent(BaseModel):
    resource_id: str
    field_id: str
    conversation_page: int
    value: dict[str, Any]


def _marklogic_txn(
    txn: Transaction,
    *,
    kbid: str,
    ensure_writes: bool = False,
) -> MarkLogicTransaction:
    documents.driver_txn(txn, ensure_writes=ensure_writes)
    assert txn.kbid == kbid
    return txn


async def create_page(txn: Transaction, data: ConversationPageContent) -> None:
    documents.driver_txn(txn, ensure_writes=True)
    await documents.write(
        txn,
        uri=MarkLogicURIs.conversation_page(data.resource_id, data.field_id, data.conversation_page),
        collection=MarkLogicCollections.CONVERSATIONS,
        content=data.model_dump(mode="json"),
    )


@observer.wrap({"type": "conversation", "op": "get_metadata"})
async def get_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
) -> FieldConversation | None:
    marklogic_txn = _marklogic_txn(txn, kbid=kbid)
    content = await documents.read(marklogic_txn, MarkLogicURIs.field(rid, "c", field_id))
    if content is None:
        return None
    value = content.get("value")
    return documents.from_json(value, FieldConversation) if isinstance(value, dict) else None


@observer.wrap({"type": "conversation", "op": "set_metadata"})
async def set_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
    metadata: FieldConversation,
    status: FieldStatus | None = None,
) -> None:
    marklogic_txn = _marklogic_txn(txn, kbid=kbid, ensure_writes=True)
    update = DocumentUpdate(
        uri=MarkLogicURIs.field(rid, "c", field_id),
        collection=MarkLogicCollections.FIELDS,
    )
    update["value"] = documents.to_json(metadata)
    if status is not None:
        update["status"] = documents.to_json(status)
    await documents.update_document(marklogic_txn, update)


@observer.wrap({"type": "conversation", "op": "get_page"})
async def get_page(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
    page: int,
) -> PBConversation | None:
    if page <= _SPLITS_METADATA_PAGE:
        raise ValueError("Conversation pages start at index 1")
    marklogic_txn = _marklogic_txn(txn, kbid=kbid)
    content = await documents.read(marklogic_txn, MarkLogicURIs.conversation_page(rid, field_id, page))
    if content is None:
        return None
    value = content.get("value")
    return documents.from_json(value, PBConversation) if isinstance(value, dict) else None


@observer.wrap({"type": "conversation", "op": "set_page"})
async def set_page(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
    page: int,
    value: PBConversation,
) -> None:
    if page <= _SPLITS_METADATA_PAGE:
        raise ValueError("Conversation pages start at index 1")
    await create_page(
        txn,
        ConversationPageContent(
            resource_id=rid,
            field_id=field_id,
            conversation_page=page,
            value=documents.to_json(value),
        ),
    )


@observer.wrap({"type": "conversation", "op": "get_splits_metadata"})
async def get_splits_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
) -> SplitsMetadata | None:
    marklogic_txn = _marklogic_txn(txn, kbid=kbid)
    content = await documents.read(
        marklogic_txn,
        MarkLogicURIs.conversation_page(rid, field_id, _SPLITS_METADATA_PAGE),
    )
    if content is None:
        return None
    value = content.get("value")
    return documents.from_json(value, SplitsMetadata) if isinstance(value, dict) else None


@observer.wrap({"type": "conversation", "op": "set_splits_metadata"})
async def set_splits_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
    splits_metadata: SplitsMetadata,
) -> None:
    await create_page(
        txn,
        ConversationPageContent(
            resource_id=rid,
            field_id=field_id,
            conversation_page=_SPLITS_METADATA_PAGE,
            value=documents.to_json(splits_metadata),
        ),
    )


@observer.wrap({"type": "conversation", "op": "delete_field"})
async def delete_field(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
) -> None:
    marklogic_txn = _marklogic_txn(txn, kbid=kbid, ensure_writes=True)
    deletion = DocumentDeletion()
    deletion.uri(MarkLogicURIs.field(rid, "c", field_id))
    deletion.directory(
        MarkLogicURIs.conversation_directory(rid, field_id),
        (
            MarkLogicCollections.CONVERSATIONS,
            MarkLogicCollections.CHUNKS,
            MarkLogicCollections.EMBEDDINGS,
        ),
    )
    await documents.delete_documents(marklogic_txn, deletion)
