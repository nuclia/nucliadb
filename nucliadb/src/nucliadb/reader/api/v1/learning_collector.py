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
from fastapi import Request
from fastapi_versioning import version

from nucliadb.learning_proxy import learning_collector_proxy
from nucliadb.reader.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires


@api.get(
    path=f"/{KB_PREFIX}/{{kbid}}/feedback/{{month}}",
    status_code=200,
    summary="Download feedback of a Knowledge Box",
    description="Download the feedback of a particular month in a Knowledge Box",  # noqa
    response_model=None,
    tags=["Knowledge Boxes"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def feedback_download(
    request: Request,
    kbid: str,
    month: str,
):
    return await learning_collector_proxy(request, "GET", f"/collect/feedback/{kbid}/{month}")


@api.get(
    path=f"/{KB_PREFIX}/{{kbid}}/feedback",
    status_code=200,
    summary="Feedback avalaible months",
    description="List of months within the last year with feedback data",
    response_model=None,
    tags=["Knowledge Boxes"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def feedback_list_months(
    request: Request,
    kbid: str,
):
    return await learning_collector_proxy(request, "GET", f"/collect/feedback/{kbid}")
