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
from typing import AsyncGenerator, TypeVar, cast

import psycopg
from google.protobuf.message import Message

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction, ReadOnlyPGTransaction
from nucliadb.common.maindb.utils import get_driver

logger = logging.getLogger(__name__)

PB_TYPE = TypeVar("PB_TYPE", bound=Message)


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
    if isinstance(txn, PGTransaction):
        async with _pg(txn).connection.cursor() as cur:
            yield cur
    elif isinstance(txn, ReadOnlyPGTransaction):
        async with txn.driver._get_connection() as conn, conn.cursor() as cur:
            yield cur
    else:
        raise TypeError(f"Unsupported transaction type: {type(txn)}")
