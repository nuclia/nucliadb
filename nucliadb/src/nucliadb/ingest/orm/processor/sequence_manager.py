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
from typing import Optional

from nucliadb.common.maindb.driver import Driver, Transaction

TXNID = "/internal/worker/{worker}"


async def get_last_seqid(driver: Driver, worker: str) -> Optional[int]:
    """
    Get last stored sequence id for a worker.

    This is oriented towards the ingest consumer and processor,
    which is the only one that should be writing to this key.
    """
    async with driver.ro_transaction() as txn:
        key = TXNID.format(worker=worker)
        last_seq = await txn.get(key)
        if not last_seq:
            return None
        else:
            return int(last_seq)


async def set_last_seqid(txn: Transaction, worker: str, seqid: int) -> None:
    """
    Set last stored sequence id for a worker on a ingest consumer.
    """
    key = TXNID.format(worker=worker)
    await txn.set(key, str(seqid).encode())
