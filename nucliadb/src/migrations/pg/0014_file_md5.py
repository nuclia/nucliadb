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

from nucliadb.common.maindb.pg import PGTransaction


async def migrate(txn: PGTransaction) -> None:
    """
    Create file_md5 table to track MD5 hashes of uploaded files per knowledge box.

    This enables deduplication checks: given an MD5 hash, callers can determine
    if a file with the same content has already been uploaded to a KB.

    Note: lookups by (kbid, md5) and deletions by (kbid) are already fast
    because the PK index starts with (kbid, md5, ...). Additional indexes
    are only needed for (kbid, rid) and (kbid, rid, field_id) access patterns.
    """
    async with txn.connection.cursor() as cur:
        await cur.execute("""
            CREATE TABLE IF NOT EXISTS file_md5 (
                kbid UUID NOT NULL,
                md5 TEXT NOT NULL,
                rid UUID NOT NULL,
                field_id TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY (kbid, md5, rid, field_id)
            );
        """)

        # Fast cleanup on resource deletion
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_md5_resource
            ON file_md5(kbid, rid);
        """)

        # Fast cleanup on field deletion
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_md5_field
            ON file_md5(kbid, rid, field_id);
        """)
