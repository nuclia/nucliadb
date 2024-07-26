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

from typing import cast

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGDriver, PGTransaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb_telemetry import metrics
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature

from ..resource import Resource

observer = metrics.Observer("pg_catalog_write", labels={"type": ""})


def _pg_transaction(txn: Transaction) -> PGTransaction:
    return cast(PGTransaction, txn)


def pgcatalog_enabled(kbid):
    return isinstance(get_driver(), PGDriver) and has_feature(
        const.Features.PG_CATALOG_WRITE, context={"kbid": kbid}
    )


@observer.wrap({"type": "update"})
async def pgcatalog_update(txn: Transaction, kbid: str, resource: Resource):
    if not pgcatalog_enabled(kbid):
        return

    if resource.basic is None:
        raise ValueError("Cannot index into the catalog a resource without basic metadata ")

    async with _pg_transaction(txn).connection.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO catalog
            (kbid, rid, title, created_at, modified_at, labels)
            VALUES
            (%(kbid)s, %(rid)s, %(title)s, %(created_at)s, %(modified_at)s, %(labels)s)
            ON CONFLICT (kbid, rid) DO UPDATE SET
            title = excluded.title,
            created_at = excluded.created_at,
            modified_at = excluded.modified_at,
            labels = excluded.labels""",
            {
                "kbid": resource.kb.kbid,
                "rid": resource.uuid,
                "title": resource.basic.title,
                "created_at": resource.basic.created.ToDatetime(),
                "modified_at": resource.basic.modified.ToDatetime(),
                "labels": list(resource.indexer.brain.labels),
            },
        )


@observer.wrap({"type": "delete"})
async def pgcatalog_delete(txn: Transaction, kbid: str, rid: str):
    if not pgcatalog_enabled(kbid):
        return

    async with _pg_transaction(txn).connection.cursor() as cur:
        await cur.execute(
            "DELETE FROM catalog where kbid = %(kbid)s AND rid = %(rid)s", {"kbid": kbid, "rid": rid}
        )
