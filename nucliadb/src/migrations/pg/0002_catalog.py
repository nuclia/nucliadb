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
    async with txn.connection.cursor() as cur:
        await cur.execute(r"""
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            CREATE EXTENSION IF NOT EXISTS btree_gin;
            CREATE TABLE catalog (
                kbid UUID,
                rid UUID,
                title TEXT,
                created_at TIMESTAMP,
                modified_at TIMESTAMP,
                labels TEXT[],
                PRIMARY KEY(kbid, rid)
            );
            CREATE INDEX ON catalog USING GIN(kbid, labels);
            CREATE INDEX ON catalog USING GIN(kbid, regexp_split_to_array(lower(title), '\W'::text));
            CREATE INDEX ON catalog(kbid, created_at);
            CREATE INDEX ON catalog(kbid, modified_at);
        """)
