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

from fastapi import HTTPException, Request
from fastapi_versioning import version

from nucliadb.common import datamanagers
from nucliadb.train.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.trainset import TrainSetPartitions
from nucliadb_utils.authentication import requires_one


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/trainset",
    tags=["Train"],
    status_code=200,
    summary="Return Train call",
    response_model=TrainSetPartitions,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def get_all_partitions(request: Request, kbid: str) -> TrainSetPartitions:
    return await get_partitions(kbid)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/trainset/{{prefix}}",
    tags=["Train"],
    status_code=200,
    summary="Return Train call",
    response_model=TrainSetPartitions,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def get_partitions_by_prefix(request: Request, kbid: str, prefix: str) -> TrainSetPartitions:
    return await get_partitions(kbid, prefix=prefix)


async def get_partitions(kbid: str, prefix: str | None = None) -> TrainSetPartitions:
    shards = await datamanagers.atomic.kb.get_shards(kbid=kbid)

    if shards is None:
        # Most likely, KB doesn't exist
        raise HTTPException(status_code=404, detail=f"No shards found for kb")

    selected = []
    prefix = prefix or ""
    for shard in shards.shards:
        if shard.shard.startswith(prefix):
            selected.append(shard.shard)

    return TrainSetPartitions(partitions=selected)
