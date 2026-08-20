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
import contextlib
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Final, TypeVar, cast

import psycopg
import psycopg.sql
from google.protobuf.message import Message

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxConflict
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction, ReadOnlyPGTransaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb_telemetry.metrics import Observer

logger = logging.getLogger(__name__)


observer = Observer(
    "nucliadb_datamanagers",
    error_mappings={
        "conflict": KnowledgeBoxConflict,
    },
    labels={"type": "unknown", "op": "unknown"},
)


PB_TYPE = TypeVar("PB_TYPE", bound=Message)


class _UnsetType:
    pass


UNSET: Final = _UnsetType()


async def get_kv_pb(
    txn: Transaction, key: str, pb_type: type[PB_TYPE], for_update: bool = True
) -> PB_TYPE | None:
    serialized: bytes | None = await txn.get(key, for_update=for_update)
    if serialized is None:
        return None
    pb = pb_type()
    pb.ParseFromString(serialized)
    return pb


@contextlib.asynccontextmanager
async def with_rw_transaction():
    driver = get_driver()
    async with driver.rw_transaction() as txn:
        yield txn


# For backwards compatibility
with_transaction = with_rw_transaction


@contextlib.asynccontextmanager
async def with_ro_transaction():
    driver = get_driver()
    async with driver.ro_transaction() as ro_txn:
        yield ro_txn


def _pg(txn: Transaction) -> PGTransaction:
    return cast(PGTransaction, txn)


@asynccontextmanager
async def _pg_cursor(txn: Transaction) -> AsyncGenerator[psycopg.AsyncCursor]:
    """Return a regular client-side cursor.

    Use this for point lookups and small result sets, where the simplicity of a
    standard cursor is preferable and materializing the result client-side is not
    a scaling concern.
    """
    if isinstance(txn, PGTransaction):
        async with _pg(txn).connection.cursor() as cur:
            yield cur
    elif isinstance(txn, ReadOnlyPGTransaction):
        async with txn.driver._get_connection() as conn, conn.cursor() as cur:
            yield cur
    else:
        raise TypeError(f"Unsupported transaction type: {type(txn)}")


@asynccontextmanager
async def _pg_server_cursor(
    txn: Transaction,
    *,
    name: str,
    batch_size: int | None = None,
) -> AsyncGenerator[psycopg.AsyncServerCursor]:
    """Return a server-side cursor for batched iteration over large result sets.

    Use this when rows should be consumed incrementally to reduce client memory
    usage and avoid materializing the full result set at once. `batch_size`
    controls how many rows psycopg fetches per round trip while iterating.
    """
    if isinstance(txn, PGTransaction):
        async with _pg(txn).connection.cursor(name=name) as cur:
            if batch_size is not None:
                cur.itersize = batch_size
            yield cur
    elif isinstance(txn, ReadOnlyPGTransaction):
        async with txn.driver._get_connection() as conn, conn.cursor(name=name) as cur:
            if batch_size is not None:
                cur.itersize = batch_size
            yield cur
    else:
        raise TypeError(f"Unsupported transaction type: {type(txn)}")
