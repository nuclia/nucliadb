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
    # Concurrent index must be created outside of a transaction but psycopg automatically
    # creates transactions. We temporarily disable this for building indexes.
    await txn.connection.commit()
    try:
        await txn.connection.set_autocommit(True)
        # btree index on title for sorting
        await txn.connection.execute("CREATE INDEX CONCURRENTLY ON catalog (title);")
        # trigram index on title for fuzzy search
        await txn.connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        await txn.connection.execute(
            "CREATE INDEX CONCURRENTLY ON catalog USING gist (title gist_trgm_ops);"
        )
        await txn.connection.execute("ANALYZE catalog")
    finally:
        await txn.connection.set_autocommit(False)
