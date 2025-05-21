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
import time

import orjson
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.routing import APIRouter
from fastapi_versioning import version
from jwcrypto import jwe, jwk  # type: ignore

from nucliadb.common.http_clients.auth import NucliaAuthHTTPClient
from nucliadb.standalone import versions
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires
from nucliadb_utils.settings import nuclia_settings

from .settings import Settings

logger = logging.getLogger(__name__)

standalone_api_router = APIRouter()


@standalone_api_router.get("/config-check")
@version(1)
@requires(NucliaDBRoles.READER)
async def api_config_check(request: Request):
    valid_nua_key = False
    nua_key_check_error = None
    if nuclia_settings.nuclia_service_account is not None:
        async with NucliaAuthHTTPClient() as auth_client:
            try:
                resp = await auth_client.status()
                if resp.auth != "nua_key":
                    logger.warning(f"Invalid nua key type: {resp}")
                    nua_key_check_error = f"Invalid nua key type: {resp.auth}"
                else:
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


TEMP_TOKEN_EXPIRATION = 5 * 60


@standalone_api_router.get("/temp-access-token")
@version(1)
@requires([NucliaDBRoles.READER, NucliaDBRoles.WRITER, NucliaDBRoles.MANAGER])
def get_temp_access_token(request: Request):
    claims = {
        "iat": int(time.time()),
        "exp": int(time.time() + TEMP_TOKEN_EXPIRATION),
        "scopes": request.auth.scopes,
        "username": request.user.display_name,
    }
    payload = orjson.dumps(claims)
    jwetoken = jwe.JWE(payload, orjson.dumps({"alg": "A256KW", "enc": "A256CBC-HS512"}))
    settings: Settings = request.app.settings
    if settings.jwk_key is None:
        logger.warning(
            "Dynamically generating JWK key. Please set JWK_KEY env variable to avoid this message."
        )
        settings.jwk_key = orjson.dumps(jwk.JWK.generate(kty="oct", size=256, kid="dyn")).decode("utf-8")
    jwetoken.add_recipient(jwk.JWK(**orjson.loads(settings.jwk_key)))
    token = jwetoken.serialize(compact=True)
    return JSONResponse({"token": token})


@standalone_api_router.get("/health/alive")
async def alive(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


@standalone_api_router.get("/health/ready")
async def ready(request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


@standalone_api_router.get("/versions")
async def versions_endpoint(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            package: {
                "installed": versions.get_installed_version(package),
                "latest": await versions.get_latest_version(package),
            }
            for package in versions.WatchedPackages
        }
    )
