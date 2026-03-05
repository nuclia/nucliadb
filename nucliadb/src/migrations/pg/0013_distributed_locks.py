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
    Create distributed_locks table for PostgreSQL-based distributed locking.

    The table is created as UNLOGGED for maximum performance since:
    - Lock data is ephemeral and doesn't need durability guarantees
    - Locks are automatically expired and cleaned up
    - No need for WAL logging as data loss on crash is acceptable
    - Significantly reduces I/O overhead for lock operations
    """
    async with txn.connection.cursor() as cur:
        await cur.execute("""
            CREATE UNLOGGED TABLE IF NOT EXISTS distributed_locks (
                lock_key TEXT PRIMARY KEY,
                lock_value TEXT NOT NULL,
                expires_at DOUBLE PRECISION NOT NULL
            );
        """)

        # Index on expires_at to efficiently find expired locks for cleanup
        await cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_distributed_locks_expires_at
            ON distributed_locks(expires_at);
        """)
