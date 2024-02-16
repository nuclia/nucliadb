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
from enum import Enum
from typing import Any, Optional, Union

from fastapi.datastructures import QueryParams
from fastapi.responses import JSONResponse, StreamingResponse

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.datamanagers.kb import KnowledgeBoxDataManager
from nucliadb.common.maindb.utils import get_driver
from nucliadb.search.predict import PredictEngine
from nucliadb.search.utilities import get_predict


class PredictProxiedEndpoints(str, Enum):
    """
    Enum for the different endpoints that are proxied to the Predict API
    """

    TOKENS = "tokens"
    CHAT = "chat"
    REPHRASE = "rephrase"


async def predict_proxy(
    kbid: str,
    endpoint: PredictProxiedEndpoints,
    method: str,
    params: QueryParams,
    json: Optional[Any] = None,
) -> Union[JSONResponse, StreamingResponse]:
    if not await exists_kb(kbid):
        raise KnowledgeBoxNotFound()

    predict: PredictEngine = get_predict()

    # Add KB configuration headers
    headers = predict.get_predict_headers(kbid)

    # Proxy the request to predict API
    predict_response = await predict.make_request(
        method=method,
        url=predict.get_predict_url(endpoint, kbid),
        json=json,
        params=params,
        headers=headers,
    )

    # Proxy the response back to the client
    status_code = predict_response.status
    response: Union[JSONResponse, StreamingResponse]
    if predict_response.headers.get("Transfer-Encoding") == "chunked":
        response = StreamingResponse(
            content=predict_response.content.iter_any(),
            status_code=status_code,
            media_type=predict_response.headers.get("Content-Type"),
        )
    else:
        response = JSONResponse(
            content=await predict_response.json(),
            status_code=status_code,
        )
    nuclia_learning_id = predict_response.headers.get("NUCLIA-LEARNING-ID")
    if nuclia_learning_id:
        response.headers["NUCLIA-LEARNING-ID"] = nuclia_learning_id
        response.headers["Access-Control-Expose-Headers"] = "NUCLIA-LEARNING-ID"
    return response


async def exists_kb(kbid: str) -> bool:
    driver = get_driver()
    kbdm = KnowledgeBoxDataManager(driver)
    return await kbdm.exists_kb(kbid)
