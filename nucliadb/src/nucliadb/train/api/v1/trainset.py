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

from fastapi import Request
from fastapi_versioning import version

from nucliadb.train.api.utils import get_kb_partitions
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
async def get_partitions_all(request: Request, kbid: str) -> TrainSetPartitions:
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
async def get_partitions_prefix(request: Request, kbid: str, prefix: str) -> TrainSetPartitions:
    return await get_partitions(kbid, prefix=prefix)


async def get_partitions(kbid: str, prefix: Optional[str] = None) -> TrainSetPartitions:
    all_keys = await get_kb_partitions(kbid, prefix)
    return TrainSetPartitions(partitions=all_keys)
