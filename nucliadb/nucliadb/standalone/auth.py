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
from typing import Optional, Tuple

import orjson
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


class AuthHeaderAuthenticationBackend(AuthenticationBackend):
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.user_header = settings.auth_policy_user_header

    async def authenticate(
        self, request: HTTPConnection
    ) -> Optional[Tuple[AuthCredentials, BaseUser]]:
        if self.user_header not in request.headers:
            return None

        user = request.headers[self.user_header]
        nuclia_user: BaseUser = NucliaUser(username=user)

        auth_creds = AuthCredentials(
            get_mapped_roles(settings=self.settings, data={"user": user})
        )

        return auth_creds, nuclia_user


_AUTHORIZATION_HEADER = "Authorization"


class OAuth2AuthenticationBackend(AuthenticationBackend):
    """
    Remember, this is still naive and expects upstream to validate
    the request before proxying here.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def authenticate(
        self, request: HTTPConnection
    ) -> Optional[Tuple[AuthCredentials, BaseUser]]:
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
            logger.warning(
                f"Could not parse jwt bearer token value: {token}", exc_info=True
            )
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


class BasicAuthAuthenticationBackend(AuthenticationBackend):
    """
    Remember, this is still naive and expects upstream to validate
    the request before proxying here.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.default_roles = settings.auth_policy_user_default_roles

    async def authenticate(
        self, request: HTTPConnection
    ) -> Optional[Tuple[AuthCredentials, BaseUser]]:
        if _AUTHORIZATION_HEADER not in request.headers:
            return None

        auth_type, _, auth_data = request.headers[_AUTHORIZATION_HEADER].partition(" ")
        if auth_type.lower() != "basic":
            return None

        token = base64.b64decode(auth_data).decode("utf-8")
        user = token.split(":")[0]

        nuclia_user: BaseUser = NucliaUser(username=user)
        auth_creds = AuthCredentials(
            get_mapped_roles(settings=self.settings, data={"user": user})
        )

        return auth_creds, nuclia_user


def get_auth_backend(settings: Settings) -> AuthenticationBackend:
    if settings.auth_policy == AuthPolicy.UPSTREAM_NAIVE:
        return NucliaCloudAuthenticationBackend(
            roles_header=settings.auth_policy_roles_header,
            user_header=settings.auth_policy_user_header,
        )
    elif settings.auth_policy == AuthPolicy.UPSTREAM_AUTH_HEADER:
        return AuthHeaderAuthenticationBackend(settings)
    elif settings.auth_policy == AuthPolicy.UPSTREAM_OAUTH2:
        return OAuth2AuthenticationBackend(settings)
    elif settings.auth_policy == AuthPolicy.UPSTREAM_BASICAUTH:
        return BasicAuthAuthenticationBackend(settings)
    else:
        raise ConfigurationError(f"Unsupported auth policy: {settings.auth_policy}")
