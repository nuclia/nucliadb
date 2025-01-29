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

from fastapi import Response
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb import learning_proxy
from nucliadb.ingest.orm.exceptions import VectorSetConflict
from nucliadb.models.responses import HTTPConflict
from nucliadb.writer import vectorsets
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import (
    NucliaDBRoles,
)
from nucliadb_utils.authentication import requires_one


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/vectorsets/{{vectorset_id}}",
    status_code=200,
    summary="Add a vectorset to Knowledge Box",
    tags=["Knowledge Boxes"],
    # TODO: remove when the feature is mature
    include_in_schema=False,
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def add_vectorset(request: Request, kbid: str, vectorset_id: str) -> Response:
    try:
        await vectorsets.add(kbid, vectorset_id)
    except learning_proxy.ProxiedLearningConfigError as err:
        return Response(
            status_code=err.status_code,
            content=err.content,
            media_type=err.content_type,
        )
    return Response(status_code=200)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/vectorsets/{{vectorset_id}}",
    status_code=200,
    summary="Delete vectorset from Knowledge Box",
    tags=["Knowledge Boxes"],
    # TODO: remove when the feature is mature
    include_in_schema=False,
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def delete_vectorset(request: Request, kbid: str, vectorset_id: str) -> Response:
    try:
        await vectorsets.delete(kbid, vectorset_id)
    except VectorSetConflict as exc:
        return HTTPConflict(detail=str(exc))
    except learning_proxy.ProxiedLearningConfigError as err:
        return Response(
            status_code=err.status_code,
            content=err.content,
            media_type=err.content_type,
        )
    return Response(status_code=200)
