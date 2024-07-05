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
from typing import Optional, Type, TypeVar

from google.protobuf.message import Message

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver

PB_TYPE = TypeVar("PB_TYPE", bound=Message)


async def get_kv_pb(
    txn: Transaction, key: str, pb_type: Type[PB_TYPE], for_update: bool = True
) -> Optional[PB_TYPE]:
    serialized: Optional[bytes] = await txn.get(key, for_update=for_update)
    if serialized is None:
        return None
    pb = pb_type()
    pb.ParseFromString(serialized)
    return pb


@contextlib.asynccontextmanager
async def with_rw_transaction(wait_for_abort: bool = True):
    driver = get_driver()
    async with driver.transaction(read_only=False, wait_for_abort=wait_for_abort) as txn:
        yield txn


# For backwards compatibility
with_transaction = with_rw_transaction


@contextlib.asynccontextmanager
async def with_ro_transaction(wait_for_abort: bool = True):
    driver = get_driver()
    async with driver.transaction(read_only=True, wait_for_abort=wait_for_abort) as ro_txn:
        yield ro_txn
