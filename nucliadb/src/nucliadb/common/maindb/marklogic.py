"""MarkLogic implementation of the maindb key/value driver contract."""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

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


class MarkLogicTransaction(Transaction):
    driver: MarkLogicDriver
    read_only: bool = False

    def __init__(self, driver: MarkLogicDriver, *, kbid: str | None = None):
        self.driver = driver
        self.kbid = kbid
        self._database = driver.system_database if kbid is None else driver.kb_database(kbid)
        self._data = driver.data(self._database)
        self._sdk_transaction: SDKTransaction | None = None
        self._transaction_lock = asyncio.Lock()
        self.open = True

    @property
    def database(self) -> str:
        return self._database

    @property
    def is_system_scope(self) -> bool:
        return self.kbid is None

    async def sdk_transaction(self) -> SDKTransaction | None:
        async with self._transaction_lock:
            if not self.open:
                raise RuntimeError("Transaction is closed")
            if self.read_only:
                return None
            if self._sdk_transaction is None:
                self._sdk_transaction = await self.driver.client.transactions.create(
                    database=self.database
                )
            return self._sdk_transaction

    async def params(self) -> dict[str, str]:
        transaction = await self.sdk_transaction()
        params = {"database": self.database}
        if transaction is not None:
            params["txid"] = transaction.id
        return params

    async def abort(self) -> None:
        async with self._transaction_lock:
            if self.open:
                self.open = False
                if self._sdk_transaction is not None:
                    await self._sdk_transaction.rollback()
                    self._sdk_transaction = None

    async def commit(self) -> None:
        if self.read_only:
            raise RuntimeError("Cannot commit transaction in read only mode")
        async with self._transaction_lock:
            if self.open:
                self.open = False
                if self._sdk_transaction is not None:
                    await self._sdk_transaction.commit()
                    self._sdk_transaction = None

    async def batch_get(self, keys: list[str], for_update: bool = False) -> list[bytes | None]:
        result = await self.driver.client.documents.read(
            [self._data._uri(key) for key in keys],
            params=await self.params(),
        )
        documents = {document.uri: document for document in result}
        values: list[bytes | None] = []
        for key in keys:
            document = documents.get(self._data._uri(key))
            values.append(self._data._decode(document) if document is not None else None)
        return values

    async def get(self, key: str, for_update: bool = False) -> bytes | None:
        values = await self.batch_get([key], for_update=for_update)
        return values[0]

    async def set(self, key: str, value: bytes) -> None:
        database = self.database
        transaction = await self.sdk_transaction()
        if transaction is None:
            raise RuntimeError("Cannot set in read only transaction")
        await self.driver.client.documents.write(
            Document(
                uri=self._data._uri(key),
                content=self._data._encode(key, value),
                collections=[MarkLogicCollections.MAINDB],
                content_type="application/json",
            ),
            tx=transaction,
            params={"database": database},
        )

    async def insert(self, key: str, value: bytes) -> None:
        if await self.get(key) is not None:
            raise ConflictError(key)
        await self.set(key, value)

    async def delete(self, key: str) -> None:
        if self.read_only:
            raise RuntimeError("Cannot delete in read only transaction")
        await self.driver.client.documents.delete(self._data._uri(key), params=await self.params())

    async def delete_by_prefix(self, prefix: str) -> None:
        if await self.sdk_transaction() is None:
            raise RuntimeError("Cannot delete in read only transaction")
        upper = self._data._prefix_upper_bound(prefix)
        clauses = [
            f"cts.collectionQuery({json.dumps(MarkLogicCollections.MAINDB)})",
            f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '>=', {json.dumps(prefix)})",
        ]
        if upper is not None:
            clauses.append(
                f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '<', {json.dumps(upper)})"
            )
        dsl = f"op.fromDocUris(cts.andQuery([{', '.join(clauses)}])).remove()"
        await self.driver.client.rows.update(dsl=dsl, params=await self.params())

    async def keys(
        self,
        match: str,
        count: int = DEFAULT_SCAN_LIMIT,
        include_start: bool = True,
    ) -> AsyncGenerator[str]:
        upper = self._data._prefix_upper_bound(match)
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
        uris = await self.driver.client.eval(javascript=javascript, params=await self.params())
        yielded = 0
        for uri in uris:
            key = self._data._key(str(uri))
            if not include_start and key == match:
                continue
            yield key
            yielded += 1
            if count > 0 and yielded >= count:
                return

    async def count(self, match: str) -> int:
        lower = match
        upper = self._data._prefix_upper_bound(match)
        clauses = [
            f"cts.collectionQuery({json.dumps(MarkLogicCollections.MAINDB)})",
            f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '>=', {json.dumps(lower)})",
        ]
        if upper is not None:
            clauses.append(
                f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.MAINDB_KEY)}, '<', {json.dumps(upper)})"
            )
        javascript = f"fn.count(cts.uris('', ['document'], cts.andQuery([{', '.join(clauses)}])))"
        result = await self.driver.client.eval(javascript=javascript, params=await self.params())
        return int(result[0] if len(result) > 0 else 0)


class ReadOnlyMarkLogicTransaction(MarkLogicTransaction):
    read_only: bool = True

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
        system_database: str = "nucliadb-system",
        port: int = 8000,
        admin_port: int = 8002,
        kb_database_prefix: str = "nucliadb-kb-",
    ):
        self.uri = uri
        self.username = username
        self.password = password
        self.system_database = system_database
        self.port = port
        self.admin_port = admin_port
        self.kb_database_prefix = kb_database_prefix
        self._client: Client | None = None
        self._lock = asyncio.Lock()
        self._provisioned: set[str] = set()
        self._provisioning_lock = asyncio.Lock()

    @property
    def client(self) -> Client:
        if self._client is None:
            raise RuntimeError("MarkLogic driver is not initialized")
        return self._client

    def data(self, database: str) -> MarkLogicDataLayer:
        if self._client is None:
            raise RuntimeError("MarkLogic driver is not initialized")
        return MarkLogicDataLayer(self._client, database)

    def kb_database(self, kbid: str) -> str:
        return f"{self.kb_database_prefix}{kbid}"

    @asynccontextmanager
    async def admin_client(self) -> AsyncGenerator[AdminClient]:
        async with AdminClient(f"{self.uri}:{self.admin_port}", self.username, self.password) as client:
            yield client

    async def initialize(self) -> None:
        async with self._lock:
            if self.initialized:
                return
            self._client = Client(f"{self.uri}:{self.port}", self.username, self.password)
            self.initialized = True

    async def finalize(self) -> None:
        async with self._lock:
            self.initialized = False
            if self._client is not None:
                await self._client.aclose()
            self._client = None
            self._provisioned.clear()

    @asynccontextmanager
    async def transaction(
        self, kbid: str | None = None, system: bool = False, *, read_only: bool = True
    ) -> AsyncGenerator[MarkLogicTransaction]:
        if system and kbid:
            raise ValueError("Cannot specify both system and kbid for a transaction")
        if not self.initialized or self._client is None:
            raise RuntimeError("MarkLogic driver is not initialized")
        txn = (
            ReadOnlyMarkLogicTransaction(self, kbid=kbid)
            if read_only
            else MarkLogicTransaction(self, kbid=kbid)
        )
        try:
            yield txn
        finally:
            if txn.open:
                await txn.abort()
