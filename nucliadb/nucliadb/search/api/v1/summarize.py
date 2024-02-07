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
from typing import Union

from fastapi import Request
from fastapi_versioning import version

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search.summarize import summarize
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import SummarizedResponse, SummarizeRequest
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/summarize",
    status_code=200,
    name="Summarize Your Documents",
    summary="Summarize Your Documents",
    description="Summarize Your Documents",
    tags=["Search"],
    response_model=SummarizedResponse,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def summarize_endpoint(
    request: Request,
    kbid: str,
    item: SummarizeRequest,
) -> Union[SummarizedResponse, HTTPClientError]:
    try:
        return await summarize(kbid, item)
    except KnowledgeBoxNotFound:
        return HTTPClientError(status_code=404, detail="Knowledge box not found")
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
    except predict.ProxiedPredictAPIError as err:
        return HTTPClientError(
            status_code=503,
            detail=f"Summarize service unavailable. {err.status}: {err.detail}",
        )
