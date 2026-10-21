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
from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, TypeVar

from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message

from nucliadb.common.marklogic.client import Document
from nucliadb.common.marklogic.document_deletion import DocumentDeletion
from nucliadb.common.marklogic.document_update import DocumentUpdate, patch_matching_optic
from nucliadb.common.marklogic.exceptions import DatabaseDoesNotExist

if TYPE_CHECKING:
    from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction

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
    txn: MarkLogicTransaction, ensure_writes: bool = False
) -> tuple[MarkLogicDriver, MarkLogicTransaction]:
    from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction

    if not isinstance(txn, MarkLogicTransaction) or not isinstance(txn.driver, MarkLogicDriver):
        raise TypeError("KnowledgeBox datamanager requires MarkLogicDriver")
    driver = txn.driver
    if not txn.open:
        raise RuntimeError("Transaction is closed")
    if ensure_writes and txn.read_only:
        raise RuntimeError("Cannot write in read only transaction")
    return driver, txn


async def read(txn: MarkLogicTransaction, uri: str) -> dict | None:
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


async def read_properties(txn: MarkLogicTransaction, uri: str, properties: Sequence[str]) -> dict | None:
    """Read selected top-level JSON properties using Optic; absent properties are None."""
    return (await read_many_properties(txn, [uri], properties)).get(uri)


async def read_many_properties(
    txn: MarkLogicTransaction, uris: Sequence[str], properties: Sequence[str]
) -> dict[str, dict]:
    """Project properties for found URIs using Optic; absent properties are None."""
    if not uris:
        return {}
    driver_txn(txn)
    rows = [{"uri": uri} for uri in dict.fromkeys(uris)]
    names = list(dict.fromkeys(properties))
    columns = []
    for index, name in enumerate(names):
        literal = "'" + name.replace("'", "''") + "'"
        path = f"/object-node()/node()[name(.) = {literal}]"
        columns.append(f"op.as('p{index}', op.xpath('doc', {json.dumps(path)}))")
    selection = ", ".join(["'uri'", *columns])
    javascript = (
        "const op = require('/MarkLogic/optic'); "
        f"const names = {json.dumps(names)}; "
        f"Sequence.from(op.fromLiterals({json.dumps(rows)})"
        ".joinDoc(op.col('doc'), op.col('uri'))"
        f".select([{selection}]).result().toArray().map(row => {{ "
        "const content = {}; "
        "names.forEach((name, index) => { "
        "const value = row['p' + index]; "
        "content[name] = value === undefined || value === null ? null : JSON.parse(xdmp.toJsonString(value)); "
        "}); return {uri: row.uri, content}; }))"
    )
    result = await evaluate(txn, javascript)
    contents: dict[str, dict] = {}
    for row in result:
        if not isinstance(row, dict) or not isinstance(row.get("uri"), str):
            raise RuntimeError("Invalid projected document response")
        if not isinstance(row.get("content"), dict):
            raise RuntimeError(f"Invalid projected document content: {row['uri']}")
        contents[row["uri"]] = row["content"]
    return contents


async def read_many(txn: MarkLogicTransaction, uris: list[str]) -> dict[str, dict]:
    """Read multiple JSON documents and return the found contents keyed by URI."""
    if not uris:
        return {}
    driver, marklogic_txn = driver_txn(txn)
    try:
        result = await driver.client.documents.read(
            uris,
            tx=await marklogic_txn.sdk_transaction(),
            params=await marklogic_txn.params(),
        )
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise
        return {}

    contents: dict[str, dict] = {}
    for document in result:
        if not isinstance(document, Document):
            raise RuntimeError("Invalid document response while reading multiple documents")
        if not isinstance(document.content, dict):
            raise RuntimeError(f"Invalid document content: {document.uri}")
        contents[document.uri] = dict(document.content)
    return contents


async def exists(txn: MarkLogicTransaction, uri: str) -> bool:
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


async def write(txn: MarkLogicTransaction, uri: str, collection: str, content: dict) -> None:
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
    txn: MarkLogicTransaction,
    documents_to_write: list[Document],
) -> None:
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    transaction = await marklogic_txn.sdk_transaction()
    if transaction is None:
        raise RuntimeError("Cannot write in read only transaction")
    await driver.client.documents.write(
        documents_to_write,
        tx=transaction,
        params=await marklogic_txn.params(),
    )


async def delete(txn: MarkLogicTransaction, uri: str) -> None:
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    try:
        await driver.client.documents.delete(uri, params=await marklogic_txn.params())
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise


async def evaluate(txn: MarkLogicTransaction, javascript: str) -> list[Any]:
    """Evaluate unrestricted JavaScript; callers must keep read-only queries non-mutating."""
    driver, marklogic_txn = driver_txn(txn)
    try:
        return await driver.client.eval(javascript=javascript, params=await marklogic_txn.params())
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise
        return []


async def update_rows(txn: MarkLogicTransaction, dsl: str) -> None:
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    try:
        await driver.client.rows.update(dsl=dsl, params=await marklogic_txn.params())
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise


async def update_document(txn: MarkLogicTransaction, update: DocumentUpdate) -> bool:
    """Patch root properties without reading contents; optionally create a missing document."""
    driver, marklogic_txn = driver_txn(txn, ensure_writes=True)
    if not update:
        return True
    try:
        rows = await driver.client.rows.update(
            dsl=update.to_optic(), params=await marklogic_txn.params()
        )
    except DatabaseDoesNotExist:
        if marklogic_txn.database == driver.system_database:
            raise
        rows = []
    if rows:
        return True
    if not update.upsert:
        return False
    await write(marklogic_txn, update.uri, update.collection, update.new_document())
    return True


async def delete_documents(txn: MarkLogicTransaction, deletion: DocumentDeletion) -> None:
    if not deletion:
        driver_txn(txn, ensure_writes=True)
        return
    await update_rows(txn, deletion.to_optic())


async def update_matching(txn: MarkLogicTransaction, query: str, values: dict[str, Any]) -> None:
    """Replace top-level properties of the documents matching the cts `query` expression."""
    if not values:
        driver_txn(txn, ensure_writes=True)
        return
    await update_rows(txn, patch_matching_optic(query, values))
