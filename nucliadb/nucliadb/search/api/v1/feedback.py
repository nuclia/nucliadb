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


from fastapi import Header, Request, Response
from fastapi_versioning import version

from nucliadb.models.responses import HTTPClientError
from nucliadb.search import logger, predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.utilities import get_predict
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import FeedbackRequest, NucliaDBClientType
from nucliadb_telemetry import errors
from nucliadb_utils.authentication import requires


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/feedback",
    status_code=200,
    name="Send Feedback",
    description="Send feedback for a search operation in a Knowledge Box",
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def send_feedback_endpoint(
    request: Request,
    response: Response,
    kbid: str,
    item: FeedbackRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
):
    try:
        return await send_feedback(
            kbid, item, x_nucliadb_user, x_ndb_client, x_forwarded_for
        )
    except predict.ProxiedPredictAPIError as err:
        return HTTPClientError(
            status_code=503,
            detail=f"Feedback service unavailable. {err.status}: {err.detail}",
        )
    except Exception as ex:
        errors.capture_exception(ex)
        logger.exception("Unexpected error sending feedback", extra={"kbid": kbid})
        return HTTPClientError(status_code=500, detail=f"Internal server error")


async def send_feedback(
    kbid: str,
    item: FeedbackRequest,
    x_nucliadb_user: str,
    x_ndb_client: NucliaDBClientType,
    x_forwarded_for: str,
):
    predict = get_predict()
    await predict.send_feedback(
        kbid, item, x_nucliadb_user, x_ndb_client, x_forwarded_for
    )
