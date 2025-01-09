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

from nucliadb.common.models_utils import to_proto
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import logger
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import FeedbackRequest, NucliaDBClientType
from nucliadb_telemetry import errors
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_audit


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/feedback",
    status_code=200,
    summary="Send Feedback",
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
        audit = get_audit()
        if audit is not None:
            audit.feedback(
                kbid=kbid,
                user=x_nucliadb_user,
                client_type=to_proto.client_type(x_ndb_client),
                origin=x_forwarded_for,
                learning_id=item.ident,
                good=item.good,
                task=to_proto.feedback_task(item.task),
                feedback=item.feedback,
                text_block_id=item.text_block_id,
            )
    except Exception as ex:
        errors.capture_exception(ex)
        logger.exception("Unexpected error sending feedback", extra={"kbid": kbid})
        return HTTPClientError(status_code=500, detail=f"Internal server error")
