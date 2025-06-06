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
        await cur.execute(
            """
            CREATE TABLE catalog_facets (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                kbid UUID,
                rid UUID,
                facet TEXT COLLATE ucs_basic,

                FOREIGN KEY (kbid, rid) REFERENCES catalog (kbid, rid) ON DELETE CASCADE
            );

            -- For FK checks
            CREATE INDEX ON catalog_facets(kbid, rid);

            -- Best for per-facet aggregation, also used by search with facet filter
            CREATE INDEX ON catalog_facets(kbid, facet);
            """
        )
