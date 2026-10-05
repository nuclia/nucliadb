"""MarkLogic implementation of the maindb key/value driver contract."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx

from nucliadb.common.maindb import marklogic_schema
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import DEFAULT_SCAN_LIMIT, Driver, Transaction
from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.marklogic.admin_client import AdminClient
from nucliadb.common.marklogic.client import Client, Document
from nucliadb.common.marklogic.client import Transaction as SDKTransaction

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
    def _check(response: httpx.Response, operation: str) -> None:
        if not response.is_success:
            raise RuntimeError(f"Failed to {operation}: {response.status_code} {response.text}")


class MarkLogicTransaction(Transaction):
    driver: MarkLogicDriver

    def __init__(self, driver: MarkLogicDriver, read_only: bool = False, *, kbid: str | None = None):
        self.driver = driver
        self.read_only = read_only
        self.kbid = kbid
        self.database = driver.system_database if kbid is None else driver.kb_database(kbid)
        self._transactions: dict[str, SDKTransaction] = {}
        self.open = True

    async def sdk_transaction(self, database: str) -> SDKTransaction | None:
        """A MarkLogic transaction cannot span databases, so keep one per touched database."""
        if self.read_only:
            return None
        transaction = self._transactions.get(database)
        if transaction is None:
            transaction = await self.driver.client.transactions.create(database=database)
            self._transactions[database] = transaction
        return transaction

    async def params(self, database: str) -> dict[str, str]:
        transaction = await self.sdk_transaction(database)
        params = {"database": database}
        if transaction is not None:
            params["txid"] = transaction.id
        return params

    async def abort(self) -> None:
        if self.open:
            transactions = list(self._transactions.values())
            self._transactions.clear()
            self.open = False
            for transaction in transactions:
                await transaction.rollback()

    async def commit(self) -> None:
        if self.open:
            transactions = list(self._transactions.values())
            self._transactions.clear()
            self.open = False
            for transaction in transactions:
                await transaction.commit()

    async def batch_get(self, keys: list[str], for_update: bool = False) -> list[bytes | None]:
        database = self.database
        result = await self.driver.client.documents.read(
            [self.driver.data._uri(key) for key in keys],
            tx=await self.sdk_transaction(database),
            params=await self.params(database),
        )
        if isinstance(result, httpx.Response):
            self.driver.data._check(result, "read documents")
            return [None for _ in keys]
        documents = {document.uri: document for document in result}
        values: list[bytes | None] = []
        for key in keys:
            document = documents.get(self.driver.data._uri(key))
            values.append(self.driver.data._decode(document) if document is not None else None)
        return values

    async def get(self, key: str, for_update: bool = False) -> bytes | None:
        values = await self.batch_get([key], for_update=for_update)
        return values[0]

    async def set(self, key: str, value: bytes) -> None:
        database = self.database
        transaction = await self.sdk_transaction(database)
        if transaction is None:
            raise RuntimeError("Cannot set in read only transaction")
        response = await self.driver.client.documents.write(
            Document(
                uri=self.driver.data._uri(key),
                content=self.driver.data._encode(key, value),
                collections=[MarkLogicCollections.MAINDB],
                content_type="application/json",
            ),
            tx=transaction,
            params={"database": database},
        )
        self.driver.data._check(response, "set key")

    async def insert(self, key: str, value: bytes) -> None:
        if await self.get(key) is not None:
            raise ConflictError(key)
        await self.set(key, value)

    async def delete(self, key: str) -> None:
        database = self.database
        response = await self.driver.client.documents.delete(
            self.driver.data._uri(key), params=await self.params(database)
        )
        self.driver.data._check(response, "delete key")

    async def delete_by_prefix(self, prefix: str) -> None:
        database = self.database
        if await self.sdk_transaction(database) is None:
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
        response = await self.driver.client.rows.update(dsl=dsl, params=await self.params(database))
        self.driver.data._check(response, "delete keys by prefix")

    async def keys(
        self,
        match: str,
        count: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
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
            await self.driver.client.eval(javascript=javascript, params=await self.params(self.database))
            or []
        )
        if isinstance(uris, httpx.Response):
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
        result = await self.driver.client.eval(
            javascript=javascript, params=await self.params(self.database)
        )
        if result is None:
            return 0
        if isinstance(result, httpx.Response):
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
        database: str = "nucliadb-system",
        port: int = 8000,
        admin_port: int = 8002,
        kb_database_prefix: str = "nucliadb-kb-",
    ):
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.admin_port = admin_port
        self.kb_database_prefix = kb_database_prefix
        self._client: Client | None = None
        self._data: MarkLogicDataLayer | None = None
        self._lock = asyncio.Lock()
        self._provisioned: set[str] = set()
        self._provisioning_lock = asyncio.Lock()

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

    def kb_database(self, kbid: str) -> str:
        return f"{self.kb_database_prefix}{kbid}"

    @property
    def system_database(self) -> str:
        """Persistent database for global keys and the KnowledgeBox registry."""
        return self.database

    async def ensure_system_database(self) -> None:
        async with self.admin_client() as client:
            await marklogic_schema.ensure_database(
                client, self.system_database, marklogic_schema.SYSTEM_PATH_INDEXES
            )

    @asynccontextmanager
    async def admin_client(self) -> AsyncGenerator[AdminClient]:
        async with AdminClient(f"{self.uri}:{self.admin_port}", self.username, self.password) as client:
            yield client

    async def ensure_kb_database(self, kbid: str) -> str:
        """Provision the KnowledgeBox database. Not transactional: the database outlives a rollback."""
        database = self.kb_database(kbid)
        if database in self._provisioned:
            return database
        async with self._provisioning_lock:
            if database in self._provisioned:
                return database
            async with self.admin_client() as client:
                await marklogic_schema.ensure_database(
                    client, database, marklogic_schema.KB_PATH_INDEXES
                )
            self._provisioned.add(database)
        return database

    async def delete_kb_database(self, kbid: str) -> None:
        database = self.kb_database(kbid)
        async with self.admin_client() as client:
            await client.delete_database(database)
        self._provisioned.discard(database)

    async def initialize(self) -> None:
        async with self._lock:
            if self.initialized:
                return
            self._client = Client(f"{self.uri}:{self.port}", self.username, self.password)
            self._data = MarkLogicDataLayer(self._client, self.database)
            self.initialized = True

    async def finalize(self) -> None:
        async with self._lock:
            self.initialized = False
            if self._client is not None:
                await self._client.aclose()
            self._client = None
            self._data = None
            self._provisioned.clear()

    @asynccontextmanager
    async def _transaction(
        self, *, read_only: bool, kbid: str | None = None
    ) -> AsyncGenerator[Transaction]:
        if not self.initialized or self._client is None or self._data is None:
            raise RuntimeError("MarkLogic driver is not initialized")
        if read_only:
            yield ReadOnlyMarkLogicTransaction(self, read_only=True, kbid=kbid)
            return
        txn = MarkLogicTransaction(self, kbid=kbid)
        try:
            yield txn
        finally:
            if txn.open:
                await txn.abort()
