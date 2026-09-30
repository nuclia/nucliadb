"""MarkLogic implementation of the maindb key/value driver contract."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from marklogic import Client  # type: ignore[import-untyped]
from marklogic.documents import Document  # type: ignore[import-untyped]
from marklogic.transactions import Transaction as SDKTransaction  # type: ignore[import-untyped]

from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import DEFAULT_SCAN_LIMIT, Driver, Transaction
from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths

URI_PREFIX = "maindb:"


class MarkLogicDataLayer:
    def __init__(self, client: Client, database: str):
        self.client = client
        self.database = database

    @staticmethod
    def _uri(key: str) -> str:
        return f"{URI_PREFIX}{key}"

    @staticmethod
    def _key(uri: str) -> str:
        return uri.removeprefix(URI_PREFIX)

    @staticmethod
    def _encode(key: str, value: bytes) -> dict[str, str]:
        return {"maindb_key": key, "value": base64.b64encode(value).decode("ascii")}

    @staticmethod
    def _decode(document: Document) -> bytes:
        content = document.content
        if not isinstance(content, dict):
            raise RuntimeError(f"Invalid maindb document content for {document.uri}")
        return base64.b64decode(content["value"])

    @staticmethod
    def _prefix_upper_bound(prefix: str) -> str | None:
        if not prefix:
            return None
        codepoints = list(prefix)
        for index in range(len(codepoints) - 1, -1, -1):
            if ord(codepoints[index]) < 0x10FFFF:
                return "".join([*codepoints[:index], chr(ord(codepoints[index]) + 1)])
        return None

    @staticmethod
    def _check(response: Any, operation: str) -> None:
        if not response.ok:
            raise RuntimeError(f"Failed to {operation}: {response.status_code} {response.text}")


class MarkLogicTransaction(Transaction):
    driver: MarkLogicDriver

    def __init__(self, driver: MarkLogicDriver, transaction: SDKTransaction | None):
        self.driver = driver
        self.transaction = transaction
        self.open = True

    async def abort(self) -> None:
        if self.open:
            if self.transaction is not None:
                await asyncio.to_thread(self.transaction.rollback)
            self.open = False

    async def commit(self) -> None:
        if self.open:
            if self.transaction is None:
                raise RuntimeError("Cannot commit transaction without an SDK transaction")
            await asyncio.to_thread(self.transaction.commit)
            self.open = False

    async def batch_get(self, keys: list[str], for_update: bool = False) -> list[bytes | None]:
        result = await asyncio.to_thread(
            self.driver.client.documents.read,
            [self.driver.data._uri(key) for key in keys],
            tx=self.transaction,
            params={
                "database": self.driver.database,
                **({"txid": self.transaction.id} if self.transaction is not None else {}),
            },
        )
        if not isinstance(result, list):
            self.driver.data._check(result, "read documents")
            return [None for _ in keys]
        documents = {document.uri: document for document in result}
        values: list[bytes | None] = []
        for key in keys:
            document = documents.get(self.driver.data._uri(key))
            values.append(self.driver.data._decode(document) if document else None)
        return values

    async def get(self, key: str, for_update: bool = False) -> bytes | None:
        values = await self.batch_get([key], for_update=for_update)
        return values[0]

    async def set(self, key: str, value: bytes) -> None:
        if self.transaction is None:
            raise RuntimeError("Cannot set in read only transaction")
        response = await asyncio.to_thread(
            self.driver.client.documents.write,
            Document(
                uri=self.driver.data._uri(key),
                content=self.driver.data._encode(key, value),
                collections=[MarkLogicCollections.MAINDB],
                content_type="application/json",
            ),
            tx=self.transaction,
            params={"database": self.driver.database},
        )
        self.driver.data._check(response, "set key")

    async def insert(self, key: str, value: bytes) -> None:
        if await self.get(key) is not None:
            raise ConflictError(key)
        await self.set(key, value)

    async def delete(self, key: str) -> None:
        response = await asyncio.to_thread(
            self.driver.client.delete,
            "/v1/documents",
            params={
                "database": self.driver.database,
                "uri": self.driver.data._uri(key),
                "txid": self.transaction.id if self.transaction is not None else "",
            },
        )
        self.driver.data._check(response, "delete key")

    async def delete_by_prefix(self, prefix: str) -> None:
        if self.transaction is None:
            raise RuntimeError("Cannot delete in read only transaction")
        upper = self.driver.data._prefix_upper_bound(prefix)
        clauses = [
            f"cts.collectionQuery({json.dumps(MarkLogicCollections.MAINDB)})",
            f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '>=', {json.dumps(prefix)})",
        ]
        if upper is not None:
            clauses.append(
                f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '<', {json.dumps(upper)})"
            )
        dsl = f"op.fromDocUris(cts.andQuery([{', '.join(clauses)}])).remove()"
        response = await asyncio.to_thread(
            self.driver.client.rows.update,
            dsl=dsl,
            params={"database": self.driver.database, "txid": self.transaction.id},
            return_response=True,
        )
        if not isinstance(response, list):
            self.driver.data._check(response, "delete keys by prefix")

    async def keys(
        self, match: str, count: int = DEFAULT_SCAN_LIMIT, include_start: bool = True
    ) -> AsyncGenerator[str]:
        upper = self.driver.data._prefix_upper_bound(match)
        clauses = [
            f"cts.collectionQuery({json.dumps(MarkLogicCollections.MAINDB)})",
            f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '>=', {json.dumps(match)})",
        ]
        if upper is not None:
            clauses.append(
                f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '<', {json.dumps(upper)})"
            )
        options = ["document", "item-order"]
        if count > 0:
            options.append(f"limit={count + (0 if include_start else 1)}")
        javascript = f"cts.uris('', {json.dumps(options)}, cts.andQuery([{', '.join(clauses)}]))"
        uris = (
            await asyncio.to_thread(
                self.driver.client.eval,
                javascript=javascript,
                params={
                    "database": self.driver.database,
                    **({"txid": self.transaction.id} if self.transaction is not None else {}),
                },
            )
            or []
        )
        if not isinstance(uris, list):
            self.driver.data._check(uris, "fetch keys by prefix")
            return
        yielded = 0
        for uri in uris:
            key = self.driver.data._key(str(uri))
            if not include_start and key == match:
                continue
            yield key
            yielded += 1
            if count > 0 and yielded >= count:
                return

    async def count(self, match: str) -> int:
        lower = match
        upper = self.driver.data._prefix_upper_bound(match)
        clauses = [
            f"cts.collectionQuery({json.dumps(MarkLogicCollections.MAINDB)})",
            f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '>=', {json.dumps(lower)})",
        ]
        if upper is not None:
            clauses.append(
                f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '<', {json.dumps(upper)})"
            )
        javascript = f"fn.count(cts.uris('', ['document'], cts.andQuery([{', '.join(clauses)}])))"
        result = await asyncio.to_thread(
            self.driver.client.eval,
            javascript=javascript,
            params={"database": self.driver.database},
        )
        if not isinstance(result, list):
            self.driver.data._check(result, "count keys by prefix")
            return 0
        return int(result[0] if len(result) > 0 else 0)


class ReadOnlyMarkLogicTransaction(MarkLogicTransaction):
    async def abort(self) -> None:
        self.open = False

    async def commit(self) -> None:
        raise RuntimeError("Cannot commit transaction in read only mode")

    async def set(self, key: str, value: bytes) -> None:
        raise RuntimeError("Cannot set in read only transaction")

    async def insert(self, key: str, value: bytes) -> None:
        raise RuntimeError("Cannot insert in read only transaction")

    async def delete(self, key: str) -> None:
        raise RuntimeError("Cannot delete in read only transaction")

    async def delete_by_prefix(self, prefix: str) -> None:
        raise RuntimeError("Cannot delete in read only transaction")


class MarkLogicDriver(Driver):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "nucliadb-content",
        port: int = 8000,
        admin_port: int = 8002,
    ):
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.admin_port = admin_port
        self._client: Client | None = None
        self._data: MarkLogicDataLayer | None = None
        self._lock = asyncio.Lock()

    @property
    def client(self) -> Client:
        if self._client is None:
            raise RuntimeError("MarkLogic driver is not initialized")
        return self._client

    @property
    def data(self) -> MarkLogicDataLayer:
        if self._data is None:
            raise RuntimeError("MarkLogic driver is not initialized")
        return self._data

    async def initialize(self) -> None:
        async with self._lock:
            if self.initialized:
                return
            self._client = Client(f"{self.uri}:{self.port}", digest=(self.username, self.password))
            self._data = MarkLogicDataLayer(self._client, self.database)
            self.initialized = True

    async def finalize(self) -> None:
        async with self._lock:
            self.initialized = False
            self._client = None
            self._data = None

    @asynccontextmanager
    async def _transaction(self, *, read_only: bool) -> AsyncGenerator[Transaction]:
        if not self.initialized or self._client is None or self._data is None:
            raise RuntimeError("MarkLogic driver is not initialized")
        if read_only:
            yield ReadOnlyMarkLogicTransaction(self, None)  # type: ignore[arg-type]
            return
        transaction = await asyncio.to_thread(self.client.transactions.create, database=self.database)
        txn = MarkLogicTransaction(self, transaction)
        try:
            yield txn
        finally:
            if txn.open:
                await txn.abort()
