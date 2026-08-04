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
"""
V2 implementation of file MD5 tracking using the kb_fields.md5 column
(migration 0016).

Instead of the dedicated `file_md5` table, the MD5 hash is stored directly
in the `kb_fields` row for the corresponding file field (field_type = 'f').
This avoids a separate table and keeps the hash co-located with the field data,
relying on the existing index on (kbid, md5) in kb_fields for efficient lookups.
"""

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Transaction


async def exists(*, kbid: str, md5: str) -> bool:
    """Check if a file with the given MD5 hash already exists in the KB."""
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.fields.exists_md5(txn, kbid=kbid, md5=md5, field_type="f")


async def set(txn: Transaction, *, kbid: str, md5: str, rid: str, field_id: str) -> None:
    await datamanagers.fields.set_md5(
        txn, kbid=kbid, md5=md5, rid=rid, field_id=field_id, field_type="f"
    )
