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
import json
from typing import Union

from fastapi import Request
from fastapi.responses import Response, StreamingResponse
from fastapi_versioning import version

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search.predict_proxy import PredictProxiedEndpoints, predict_proxy
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError

DESCRIPTION = "Convenience endpoint that proxies requests to the Predict API. It adds the Knowledge Box configuration settings as headers to the predict API request. Refer to the Predict API documentation for more details about the request and response models: https://docs.nuclia.dev/docs/nua-api#tag/Predict"  # noqa: E501


@api.get(
    path=f"/{KB_PREFIX}/{{kbid}}/predict/{{endpoint}}",
    status_code=200,
    summary="Predict API Proxy",
    description=DESCRIPTION,
    response_model=None,
    tags=["Search"],
)
@api.post(
    path=f"/{KB_PREFIX}/{{kbid}}/predict/{{endpoint}}",
    status_code=200,
    summary="Predict API Proxy",
    description=DESCRIPTION,
    response_model=None,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def predict_proxy_endpoint(
    request: Request,
    kbid: str,
    endpoint: PredictProxiedEndpoints,
) -> Union[Response, StreamingResponse, HTTPClientError]:
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        payload = None
    try:
        return await predict_proxy(
            kbid,
            endpoint,
            request.method,
            params=request.query_params,
            json=payload,
            headers=dict(request.headers),
        )
    except KnowledgeBoxNotFound:
        return HTTPClientError(status_code=404, detail="Knowledge box not found")
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
    except predict.ProxiedPredictAPIError as err:
        return HTTPClientError(
            status_code=err.status,
            detail=err.detail,
        )
