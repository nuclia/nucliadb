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
import base64
import logging
import time
from typing import Optional

import orjson
from jwcrypto import jwe, jwk  # type: ignore
from starlette.authentication import AuthCredentials, AuthenticationBackend, BaseUser
from starlette.requests import HTTPConnection

from nucliadb.standalone.settings import AuthPolicy, Settings
from nucliadb_utils.authentication import NucliaCloudAuthenticationBackend, NucliaUser
from nucliadb_utils.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def get_mapped_roles(*, settings: Settings, data: dict[str, str]) -> list[str]:
    output = [r.value for r in settings.auth_policy_user_default_roles]
    if settings.auth_policy_role_mapping is None:
        return output

    for property, mapping in settings.auth_policy_role_mapping.items():
        if property not in data:
            continue

        for value, roles in mapping.items():
            if data[property] == value:
                output.extend([r.value for r in roles])

    return list(set(output))


async def authenticate_auth_token(
    settings: Settings, request: HTTPConnection
) -> Optional[tuple[AuthCredentials, BaseUser]]:
    if "eph-token" not in request.query_params or settings.jwk_key is None:
        return None

    jwt_token = request.query_params["eph-token"].encode("utf-8")
    try:
        jwetoken = jwe.JWE()
        jwetoken.deserialize(jwt_token.decode("utf-8"))
        jwetoken.decrypt(jwk.JWK(**orjson.loads(settings.jwk_key)))
        payload = jwetoken.payload
    except jwe.InvalidJWEOperation:
        logger.info(f"Invalid operation", exc_info=True)
        return None
    except jwe.InvalidJWEData:
        logger.info(f"Error decrypting JWT token", exc_info=True)
        return None
    json_payload = orjson.loads(payload)
    if json_payload["exp"] <= int(time.time()):
        logger.debug(f"Expired token", exc_info=True)
        return None

    username = json_payload["username"]
    auth_creds = AuthCredentials(json_payload["scopes"])
    return auth_creds, NucliaUser(username=username)


class AuthHeaderAuthenticationBackend(NucliaCloudAuthenticationBackend):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def authenticate(self, request: HTTPConnection) -> Optional[tuple[AuthCredentials, BaseUser]]:
        token_resp = await authenticate_auth_token(self.settings, request)
        if token_resp is not None:
            return token_resp

        if self.settings.auth_policy_user_header not in request.headers:
            return None

        user = request.headers[self.settings.auth_policy_user_header]
        nuclia_user: BaseUser = NucliaUser(username=user)

        auth_creds = AuthCredentials(get_mapped_roles(settings=self.settings, data={"user": user}))

        return auth_creds, nuclia_user


_AUTHORIZATION_HEADER = "Authorization"


class OAuth2AuthenticationBackend(NucliaCloudAuthenticationBackend):
    """
    Remember, this is still naive and expects upstream to validate
    the request before proxying here.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def authenticate(self, request: HTTPConnection) -> Optional[tuple[AuthCredentials, BaseUser]]:
        token_resp = await authenticate_auth_token(self.settings, request)
        if token_resp is not None:
            return token_resp

        if _AUTHORIZATION_HEADER not in request.headers:
            return None

        token = request.headers[_AUTHORIZATION_HEADER].split(" ")[-1]
        token_split = token.split(".")
        if len(token_split) != 3:
            logger.info(f"Invalid token, expected valid jwt bearer: {token}")
            # invalid token
            return None

        try:
            token_data = orjson.loads(base64.b64decode(token_split[1] + "==="))
        except Exception:
            logger.warning(f"Could not parse jwt bearer token value: {token}", exc_info=True)
            return None

        if "sub" not in token_data:
            logger.info("JWT token missing `sub` claim")
            # invalid token
            return None

        user = token_data["sub"]
        nuclia_user: BaseUser = NucliaUser(username=user)

        auth_creds = AuthCredentials(
            get_mapped_roles(
                settings=self.settings,
                data={
                    "user": user,
                    **token_data,
                },
            )
        )

        return auth_creds, nuclia_user


class BasicAuthAuthenticationBackend(NucliaCloudAuthenticationBackend):
    """
    Remember, this is still naive and expects upstream to validate
    the request before proxying here.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def authenticate(self, request: HTTPConnection) -> Optional[tuple[AuthCredentials, BaseUser]]:
        token_resp = await authenticate_auth_token(self.settings, request)
        if token_resp is not None:
            return token_resp

        if _AUTHORIZATION_HEADER not in request.headers:
            return None

        auth_type, _, auth_data = request.headers[_AUTHORIZATION_HEADER].partition(" ")
        if auth_type.lower() != "basic":
            return None

        token = base64.b64decode(auth_data).decode("utf-8")
        user = token.split(":")[0]

        nuclia_user: BaseUser = NucliaUser(username=user)
        auth_creds = AuthCredentials(get_mapped_roles(settings=self.settings, data={"user": user}))

        return auth_creds, nuclia_user


class UpstreamNaiveAuthenticationBackend(NucliaCloudAuthenticationBackend):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        super().__init__(
            roles_header=settings.auth_policy_roles_header,
            user_header=settings.auth_policy_user_header,
        )

    async def authenticate(self, request: HTTPConnection) -> Optional[tuple[AuthCredentials, BaseUser]]:
        token_resp = await authenticate_auth_token(self.settings, request)
        if token_resp is not None:
            return token_resp

        return await super().authenticate(request)


def get_auth_backend(settings: Settings) -> AuthenticationBackend:
    if settings.auth_policy == AuthPolicy.UPSTREAM_NAIVE:
        return UpstreamNaiveAuthenticationBackend(settings)
    elif settings.auth_policy == AuthPolicy.UPSTREAM_AUTH_HEADER:
        return AuthHeaderAuthenticationBackend(settings)
    elif settings.auth_policy == AuthPolicy.UPSTREAM_OAUTH2:
        return OAuth2AuthenticationBackend(settings)
    elif settings.auth_policy == AuthPolicy.UPSTREAM_BASICAUTH:
        return BasicAuthAuthenticationBackend(settings)
    else:
        raise ConfigurationError(f"Unsupported auth policy: {settings.auth_policy}")
