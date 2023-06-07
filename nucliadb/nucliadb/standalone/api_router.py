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
import logging

from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.routing import APIRouter
from fastapi_versioning import version

from nucliadb.common.http_clients.processing import ProcessingHTTPClient
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires
from nucliadb_utils.settings import nuclia_settings

logger = logging.getLogger(__name__)

standalone_api_router = APIRouter()


@standalone_api_router.get("/config-check")
@version(1)
@requires(NucliaDBRoles.READER)
async def api_config_check(request: Request):
    valid_nua_key = False
    nua_key_check_error = None
    if nuclia_settings.nuclia_service_account is not None:
        async with ProcessingHTTPClient() as processing_client:
            try:
                await processing_client.status()
                valid_nua_key = True
            except Exception as exc:
                logger.warning(f"Error validating nua key", exc_info=exc)
                nua_key_check_error = f"Error checking NUA key: {str(exc)}"
    return JSONResponse(
        {
            "nua_api_key": {
                "has_key": nuclia_settings.nuclia_service_account is not None,
                "valid": valid_nua_key,
                "error": nua_key_check_error,
            },
            "user": {
                "username": request.user.display_name,
                "roles": request.auth.scopes,
            },
        },
    )
