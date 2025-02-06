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
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.common import datamanagers
from nucliadb.reader.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import (
    NucliaDBRoles,
)
from nucliadb_models.vectorsets import VectorSetList, VectorSetListItem
from nucliadb_utils.authentication import requires_one


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/vectorsets",
    status_code=200,
    summary="List vector sets",
    response_model=VectorSetList,
    tags=["Vector Sets"],
    # TODO: remove when the feature is mature
    include_in_schema=False,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def list_vectorsets(request: Request, kbid: str) -> VectorSetList:
    vectorsets = []
    async with datamanagers.with_ro_transaction() as txn:
        async for vid, _ in datamanagers.vectorsets.iter(txn, kbid=kbid):
            vectorsets.append(VectorSetListItem(id=vid))
    return VectorSetList(vectorsets=vectorsets)
