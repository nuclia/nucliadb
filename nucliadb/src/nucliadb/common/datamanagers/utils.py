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
import functools
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable, Coroutine, ParamSpec, TypeVar, cast

import psycopg
from google.protobuf.message import Message

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction, ReadOnlyPGTransaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature

logger = logging.getLogger(__name__)

PB_TYPE = TypeVar("PB_TYPE", bound=Message)
_P = ParamSpec("_P")
_R = TypeVar("_R")


def logs_foreign_key_error(
    func: Callable[_P, Coroutine[Any, Any, _R]],
) -> Callable[_P, Coroutine[Any, Any, _R | None]]:
    """Decorator for dual-write datamanager functions during the ORM backfill migration.

    During the backfill migration, writes are sent to both the legacy KV store and
    the new ORM (PostgreSQL) tables.  A write to an ORM table can fail with a
    ForeignKeyViolation when the parent row (e.g. the kb_resources row for a field,
    or the kbs row for a resource) has not been backfilled yet.

    This decorator catches that specific error and logs a warning instead of
    propagating the exception, so that the dual-write path does not break normal
    operation for resources and fields that are still pending migration.

    Example::

        @logs_foreign_key_error
        async def set_basic(txn, *, kbid, rid, basic): ...
    """

    @functools.wraps(func)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R | None:
        try:
            return await func(*args, **kwargs)
        except psycopg.errors.ForeignKeyViolation as exc:
            logger.warning(
                f"Foreign key violation in {wrapper.__qualname__} (parent row not yet migrated to ORM tables): {exc}"
            )
        return None

    return wrapper


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


def datamanagers_v2_write(kbid: str) -> bool:
    """
    Check if the knowledge box is currently being migrated to the v2 datamanagers.
    """
    return has_feature(const.Features.DATAMANAGERS_V2_WRITE, context={"kbid": kbid})


def datamanagers_v2_read(kbid: str) -> bool:
    """
    Check if the knowledge box has already been migrated and has datamanagers v2 reads enabled.
    """
    return has_feature(const.Features.DATAMANAGERS_V2_READ, context={"kbid": kbid})


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
