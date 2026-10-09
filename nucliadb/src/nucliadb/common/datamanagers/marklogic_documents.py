"""Transaction-scoped MarkLogic document operations for datamanagers."""

from typing import Any, TypeVar

from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.marklogic.client import Document
from nucliadb.common.marklogic.document_deletion import DocumentDeletion
from nucliadb.common.marklogic.document_update import DocumentUpdate
from nucliadb.common.marklogic.exceptions import DatabaseDoesNotExist

PB = TypeVar("PB", bound=Message)


def to_json(message: Message) -> dict:
    """Protobufs are stored as JSON so their fields stay queryable and indexable.

    Defaults are materialized so that an index on a field also matches documents
    that left it unset.
    """
    return MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )


def from_json(value: dict, pb_klass: type[PB]) -> PB:
    return ParseDict(value, pb_klass(), ignore_unknown_fields=True)


def driver_txn(
    txn: Transaction, ensure_writes: bool = False
) -> tuple[MarkLogicDriver, MarkLogicTransaction]:
    if not isinstance(txn, MarkLogicTransaction) or not isinstance(txn.driver, MarkLogicDriver):
        raise TypeError("KnowledgeBox datamanager requires MarkLogicDriver")
    driver = txn.driver
    if not txn.open:
        raise RuntimeError("Transaction is closed")
    if ensure_writes and txn.read_only:
        raise RuntimeError("Cannot write in read only transaction")
    return driver, txn


async def read(txn: Transaction, uri: str) -> dict | None:
    driver, marklogic_txn = driver_txn(txn)
    try:
        result = await driver.client.documents.read(
            uri,
            tx=await marklogic_txn.sdk_transaction(),
            params=await marklogic_txn.params(),
        )
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise
        return None
    if not result:
        return None
    document = result[0]
    if not isinstance(document, Document):
        raise RuntimeError(f"Invalid document response: {uri}")
    content = document.content
    if not isinstance(content, dict):
        raise RuntimeError(f"Invalid document content: {uri}")
    return dict(content)


async def exists(txn: Transaction, uri: str) -> bool:
    driver, marklogic_txn = driver_txn(txn)
    try:
        return await driver.client.documents.exists(
            uri,
            tx=await marklogic_txn.sdk_transaction(),
            params=await marklogic_txn.params(),
        )
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise
        return False


async def write(txn: Transaction, uri: str, collection: str, content: dict) -> None:
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    transaction = await marklogic_txn.sdk_transaction()
    if transaction is None:
        raise RuntimeError("Cannot write in read only transaction")
    await driver.client.documents.write(
        Document(uri=uri, content=content, collections=[collection], content_type="application/json"),
        tx=transaction,
        params=await marklogic_txn.params(),
    )


async def write_many(
    txn: Transaction,
    documents_to_write: list[Document],
) -> None:
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    transaction = await marklogic_txn.sdk_transaction()
    if transaction is None:
        raise RuntimeError("Cannot write in read only transaction")
    await driver.client.documents.write(
        *documents_to_write,
        tx=transaction,
        params=await marklogic_txn.params(),
    )


async def delete(txn: Transaction, uri: str) -> None:
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    try:
        await driver.client.documents.delete(uri, params=await marklogic_txn.params())
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise


async def evaluate(txn: Transaction, javascript: str) -> list[Any]:
    """Evaluate unrestricted JavaScript; callers must keep read-only queries non-mutating."""
    driver, marklogic_txn = driver_txn(txn)
    try:
        return await driver.client.eval(javascript=javascript, params=await marklogic_txn.params())
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise
        return []


async def update_rows(txn: Transaction, dsl: str) -> None:
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    try:
        await driver.client.rows.update(dsl=dsl, params=await marklogic_txn.params())
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise


async def update_document(txn: Transaction, update: DocumentUpdate) -> bool:
    """Apply `update` in place; returns False if the document is missing and was not created."""
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    if not update:
        return True
    rows = await driver.client.rows.update(dsl=update.to_optic(), params=await marklogic_txn.params())
    if rows:
        return True
    if not update.upsert:
        return False
    await write(txn, update.uri, update.collection, update.new_document())
    return True


async def delete_documents(txn: Transaction, deletion: DocumentDeletion) -> None:
    if not deletion:
        driver_txn(txn, ensure_writes=True)
        return
    await update_rows(txn, deletion.to_optic())


def require_kb_scope(txn: Transaction, kbid: str) -> None:
    driver, marklogic_txn = driver_txn(txn)
    if marklogic_txn.database != driver.kb_database(kbid):
        raise ValueError("Transaction database does not match KnowledgeBox scope")


def require_system_scope(txn: Transaction) -> None:
    driver, marklogic_txn = driver_txn(txn)
    if marklogic_txn.database != driver.system_database:
        raise ValueError("Registry operations require a system database transaction")
