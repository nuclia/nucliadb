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
from fastapi.responses import Response, StreamingResponse

from nucliadb.common import datamanagers
from nucliadb.search.predict import PredictEngine
from nucliadb.search.utilities import get_predict


class PredictProxiedEndpoints(str, Enum):
    """
    Enum for the different endpoints that are proxied to the Predict API
    """

    TOKENS = "tokens"
    CHAT = "chat"
    REPHRASE = "rephrase"
    RUN_AGENTS_TEXT = "run-agents-text"
    SUMMARIZE = "summarize"
    RERANK = "rerank"
    REMI = "remi"


ALLOWED_HEADERS = [
    "Accept",  # To allow 'application/x-ndjson' on the /chat endpoint
]


async def predict_proxy(
    kbid: str,
    endpoint: PredictProxiedEndpoints,
    method: str,
    params: QueryParams,
    json: Optional[Any] = None,
    headers: dict[str, str] = {},
) -> Union[Response, StreamingResponse]:
    if not await exists_kb(kbid=kbid):
        raise datamanagers.exceptions.KnowledgeBoxNotFound()

    predict: PredictEngine = get_predict()
    predict_headers = predict.get_predict_headers(kbid)
    user_headers = {k: v for k, v in headers.items() if k.capitalize() in ALLOWED_HEADERS}

    # Proxy the request to predict API
    predict_response = await predict.make_request(
        method=method,
        url=predict.get_predict_url(endpoint, kbid),
        json=json,
        params=params,
        headers={**user_headers, **predict_headers},
    )

    # Proxy the response back to the client
    status_code = predict_response.status
    media_type = predict_response.headers.get("Content-Type")
    response: Union[Response, StreamingResponse]
    if predict_response.headers.get("Transfer-Encoding") == "chunked":
        response = StreamingResponse(
            content=predict_response.content.iter_any(),
            status_code=status_code,
            media_type=media_type,
        )
    else:
        response = Response(
            content=await predict_response.read(),
            status_code=status_code,
            media_type=media_type,
        )
    nuclia_learning_id = predict_response.headers.get("NUCLIA-LEARNING-ID")
    if nuclia_learning_id:
        response.headers["NUCLIA-LEARNING-ID"] = nuclia_learning_id
        response.headers["Access-Control-Expose-Headers"] = "NUCLIA-LEARNING-ID"
    return response


async def exists_kb(kbid: str) -> bool:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.kb.exists_kb(txn, kbid=kbid)
