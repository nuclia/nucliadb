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
"""MarkLogic implementation of the maindb key/value driver contract."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import backoff

from nucliadb.common.maindb import marklogic_schema
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.admin_client import AdminClient
from nucliadb.common.marklogic.client import Client, Document
from nucliadb.common.marklogic.client import Transaction as SDKTransaction
from nucliadb.common.marklogic.exceptions import (
    DatabaseDoesNotExist,
    MarkLogicResponseError,
    NoHealthyUpstreamError,
)
from nucliadb.common.marklogic.uris import MarkLogicURIs

DEFAULT_SCAN_LIMIT = -1
DEFAULT_BATCH_SCAN_LIMIT = 500

DATABASE_READY_TIMEOUT = 60.0
DATABASE_READY_PROBES = 3
DATABASE_NOT_READY_CODES = {"XDMP-DBTIMESTAMP", "XDMP-FORESTNOTOPEN", "XDMP-NOSUCHDB"}
DATABASE_READY_PROBE_URI = "/nucliadb/readiness-probe.json"

logger = logging.getLogger(__name__)


class DatabaseNotReady(Exception):
    pass


def _log_not_ready(details: Any) -> None:
    logger.warning(
        "MarkLogic database not ready yet, retrying",
        extra={"database": details["args"][1], "tries": details["tries"]},
    )


class MarkLogicDataLayer:
    def __init__(self, client: Client, database: str):
        self.client = client
        self.database = database

    @staticmethod
    def _uri(key: str) -> str:
        return MarkLogicURIs.maindb_key(key)

    @staticmethod
    def _key(uri: str) -> str:
        return MarkLogicURIs.maindb_key_from_uri(uri)

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


class MarkLogicTransaction:
    driver: MarkLogicDriver

    def __init__(
        self,
        driver: MarkLogicDriver,
        *,
        kbid: str | None = None,
        read_only: bool = False,
    ):
        self.driver = driver
        self.kbid = kbid
        self.read_only = read_only
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
        if self.read_only:
            # Nothing to roll back; keeps objects cached beyond the transaction scope (e.g. resource cache) usable
            return
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
        contents = await documents.read_many(self, [self._data._uri(key) for key in keys])
        values: list[bytes | None] = []
        for key in keys:
            uri = self._data._uri(key)
            content = contents.get(uri)
            if content is None:
                values.append(None)
            else:
                values.append(base64.b64decode(content["value"]))
        return values

    async def get(self, key: str, for_update: bool = False) -> bytes | None:
        values = await self.batch_get([key], for_update=for_update)
        return values[0]

    async def set(self, key: str, value: bytes) -> None:
        if self.read_only:
            raise RuntimeError("Cannot set in read only transaction")
        await documents.write(
            self,
            uri=self._data._uri(key),
            collection=MarkLogicCollections.MAINDB,
            content=self._data._encode(key, value),
        )

    async def insert(self, key: str, value: bytes) -> None:
        if self.read_only:
            raise RuntimeError("Cannot insert in read only transaction")
        if await self.get(key) is not None:
            raise ConflictError(key)
        await self.set(key, value)

    async def delete(self, key: str) -> None:
        if self.read_only:
            raise RuntimeError("Cannot delete in read only transaction")
        await documents.delete(self, self._data._uri(key))

    async def delete_by_prefix(self, prefix: str) -> None:
        if self.read_only:
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
        await documents.update_rows(self, dsl)

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
        uris = await documents.evaluate(self, javascript)
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
        result = await documents.evaluate(self, javascript)
        return int(result[0] if len(result) > 0 else 0)


class MarkLogicDriver:
    initialized = False

    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        system_database: str = "nucliadb-system",
        port: int = 8000,
        admin_port: int = 8002,
        kb_database_prefix: str = "nucliadb-kb-",
        preallocated_partitions: int = 3,
        replication: bool = True,
    ):
        self.uri = uri
        self.username = username
        self.password = password
        self.system_database = system_database
        self.port = port
        self.admin_port = admin_port
        self.kb_database_prefix = kb_database_prefix
        self.preallocated_partitions = preallocated_partitions
        self.replication = replication
        self._client: Client | None = None
        self._lock = asyncio.Lock()
        self._provisioned: set[str] = set()
        self._provisioning_locks: dict[str, asyncio.Lock] = {}

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

    async def ensure_system_database(self) -> None:
        async with self.admin_client() as client:
            await marklogic_schema.ensure_database(
                client,
                self.system_database,
                marklogic_schema.SYSTEM_PATH_INDEXES,
                partitions=self.preallocated_partitions,
                replication=self.replication,
            )
        await self._wait_until_queryable(self.system_database)

    async def _wait_until_queryable(self, database: str) -> None:
        """A freshly created database answers XDMP-DBTIMESTAMP until its forest is open on every host."""
        try:
            await self._probe_database(database)
        except DatabaseNotReady as exc:
            raise TimeoutError(f"MarkLogic database {database} not queryable after creation") from exc

    @backoff.on_exception(
        backoff.expo,
        DatabaseNotReady,
        jitter=backoff.random_jitter,
        factor=0.1,
        max_value=2.0,
        max_time=DATABASE_READY_TIMEOUT,
        on_backoff=_log_not_ready,
    )
    async def _probe_database(self, database: str) -> None:
        # Several consecutive successes, as requests may be balanced across hosts
        for _ in range(DATABASE_READY_PROBES):
            try:
                # Plain document read rather than /v1/eval: server-side JS can crash (JS-FATAL) on new databases
                await self.client.documents.exists(
                    DATABASE_READY_PROBE_URI, params={"database": database}
                )
            except NoHealthyUpstreamError as exc:
                raise DatabaseNotReady(database) from exc
            except MarkLogicResponseError as exc:
                server_error = exc.status_code is not None and exc.status_code >= 500
                if (
                    exc.code in DATABASE_NOT_READY_CODES
                    or isinstance(exc, DatabaseDoesNotExist)
                    or server_error
                ):
                    raise DatabaseNotReady(database) from exc
                raise

    async def ensure_kb_database(self, kbid: str) -> str:
        database = self.kb_database(kbid)
        if database in self._provisioned:
            return database
        provisioning_lock = self._provisioning_locks.setdefault(database, asyncio.Lock())
        async with provisioning_lock:
            if database in self._provisioned:
                return database
            async with self.admin_client() as client:
                await marklogic_schema.ensure_database(
                    client,
                    database,
                    marklogic_schema.KB_PATH_INDEXES,
                    marklogic_schema.KB_WORD_LEXICON_PROPERTIES,
                    marklogic_schema.KB_EXCLUDED_PATHS,
                    partitions=self.preallocated_partitions,
                    replication=self.replication,
                )
            await self._wait_until_queryable(database)
            from nucliadb.common.marklogic.graph import ensure_view

            await ensure_view(self, database)
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
            try:
                await self.ensure_system_database()
            except BaseException:
                await self._client.aclose()
                self._client = None
                raise
            self.initialized = True

    async def finalize(self) -> None:
        async with self._lock:
            self.initialized = False
            if self._client is not None:
                await self._client.aclose()
            self._client = None
            self._provisioned.clear()
            self._provisioning_locks.clear()

    @asynccontextmanager
    async def transaction(
        self, kbid: str | None = None, system: bool = False, *, read_only: bool = True
    ) -> AsyncGenerator[MarkLogicTransaction]:
        if system and kbid:
            raise ValueError("Cannot specify both system and kbid for a transaction")
        if not self.initialized or self._client is None:
            raise RuntimeError("MarkLogic driver is not initialized")
        txn = MarkLogicTransaction(self, kbid=kbid, read_only=read_only)
        try:
            yield txn
        finally:
            if txn.open:
                await txn.abort()

    @asynccontextmanager
    async def ro_transaction(self, *, kbid: str | None = None) -> AsyncGenerator[MarkLogicTransaction]:
        async with self.transaction(kbid=kbid, read_only=True) as txn:
            yield txn

    @asynccontextmanager
    async def rw_transaction(self, *, kbid: str | None = None) -> AsyncGenerator[MarkLogicTransaction]:
        async with self.transaction(kbid=kbid, read_only=False) as txn:
            yield txn
