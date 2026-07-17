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

import logging
from typing import overload

from nucliadb.common import file_md5_v2
from nucliadb.common.maindb.driver import Transaction
from nucliadb_telemetry import metrics

logger = logging.getLogger(__name__)

observer = metrics.Observer("nucliadb_file_md5", labels={"op": ""})


@observer.wrap({"op": "check"})
async def exists(*, kbid: str, md5: str) -> bool:
    """Check if a file with the given MD5 hash already exists in the KB."""
    return await file_md5_v2.exists(kbid=kbid, md5=md5)


@observer.wrap({"op": "store"})
async def set(txn: Transaction, *, kbid: str, md5: str, rid: str, field_id: str) -> None:
    """Set a file MD5 hash for a resource field."""
    await file_md5_v2.set(txn, kbid=kbid, md5=md5, rid=rid, field_id=field_id)


@observer.wrap({"op": "get"})
async def get(txn: Transaction, *, kbid: str, rid: str, field_id: str) -> str | None:
    """Get the MD5 hash for a resource field, or None if not found."""
    return await file_md5_v2.get(txn, kbid=kbid, rid=rid, field_id=field_id)


@overload
async def delete(txn: Transaction, *, kbid: str) -> None: ...


@overload
async def delete(txn: Transaction, *, kbid: str, rid: str) -> None: ...


@overload
async def delete(txn: Transaction, *, kbid: str, rid: str, field_id: str) -> None: ...


@observer.wrap({"op": "delete"})
async def delete(
    txn: Transaction,
    *,
    kbid: str,
    rid: str | None = None,
    field_id: str | None = None,
) -> None:
    """Delete file MD5 records.

    - kbid only: delete all records for the KB
    - kbid + rid: delete all records for a resource
    - kbid + rid + field_id: delete records for a specific field
    """
    if rid is None:
        await file_md5_v2.delete(txn, kbid=kbid)
    elif field_id is None:
        await file_md5_v2.delete(txn, kbid=kbid, rid=rid)
    else:
        await file_md5_v2.delete(txn, kbid=kbid, rid=rid, field_id=field_id)
