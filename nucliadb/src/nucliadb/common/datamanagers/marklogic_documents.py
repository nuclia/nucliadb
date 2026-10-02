"""Transaction-scoped MarkLogic document operations for datamanagers."""

import json
from typing import Any, TypeVar

import httpx
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.marklogic.client import Document

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


def driver_txn(txn: Transaction) -> tuple[MarkLogicDriver, MarkLogicTransaction]:
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver) or not isinstance(txn, MarkLogicTransaction):
        raise TypeError("Datamanager requires MarkLogicDriver")
    return driver, txn


def missing_database(response: Any) -> bool:
    """A KnowledgeBox database only exists once the KnowledgeBox has been created."""
    return getattr(response, "status_code", None) in (400, 404) and "No such database" in getattr(
        response, "text", ""
    )


async def read(txn: Transaction, database: str, uri: str) -> dict | None:
    driver, marklogic_txn = driver_txn(txn)
    result = await driver.client.documents.read(
        uri,
        tx=await marklogic_txn.sdk_transaction(database),
        params=await marklogic_txn.params(database),
    )
    if isinstance(result, httpx.Response):
        if missing_database(result):
            return None
        driver.data._check(result, "read document")
        raise RuntimeError("Unexpected response when reading document")
    if not result:
        return None
    content = result[0].content
    if not isinstance(content, dict):
        raise RuntimeError(f"Invalid document content: {uri}")
    return dict(content)


async def write(txn: Transaction, database: str, uri: str, collection: str, content: dict) -> None:
    driver, marklogic_txn = driver_txn(txn)
    transaction = await marklogic_txn.sdk_transaction(database)
    if transaction is None:
        raise RuntimeError("Cannot write in read only transaction")
    response = await driver.client.documents.write(
        Document(uri=uri, content=content, collections=[collection], content_type="application/json"),
        tx=transaction,
        params={"database": database},
    )
    driver.data._check(response, "write document")


async def delete(txn: Transaction, database: str, uri: str) -> None:
    driver, marklogic_txn = driver_txn(txn)
    if await marklogic_txn.sdk_transaction(database) is None:
        raise RuntimeError("Cannot delete in read only transaction")
    response = await driver.client.documents.delete(uri, params=await marklogic_txn.params(database))
    if missing_database(response):
        return
    driver.data._check(response, "delete document")


async def evaluate(txn: Transaction, database: str, javascript: str) -> list:
    driver, marklogic_txn = driver_txn(txn)
    result = await driver.client.eval(javascript=javascript, params=await marklogic_txn.params(database))
    if result is None:
        return []
    if isinstance(result, httpx.Response):
        if missing_database(result):
            return []
        driver.data._check(result, "evaluate query")
        raise RuntimeError("Unexpected response when evaluating query")
    return result


async def delete_resource_children(txn: Transaction, database: str, rid: str) -> None:
    """Remove the field and conversation documents belonging to a resource."""
    driver, marklogic_txn = driver_txn(txn)
    if await marklogic_txn.sdk_transaction(database) is None:
        raise RuntimeError("Cannot delete in read only transaction")
    dsl = (
        "op.fromDocUris(cts.andQuery(["
        "cts.collectionQuery(['fields', 'conversations']), "
        f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.RID)}, '=', {json.dumps(rid)})"
        "])).remove()"
    )
    response = await driver.client.rows.update(dsl=dsl, params=await marklogic_txn.params(database))
    if not missing_database(response):
        driver.data._check(response, "delete resource children")
