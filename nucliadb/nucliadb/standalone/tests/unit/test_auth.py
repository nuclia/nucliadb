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

import time
from unittest.mock import Mock

import orjson
import pytest
from jwcrypto import jwe, jwk  # type: ignore

from nucliadb.standalone import auth
from nucliadb.standalone.settings import AuthPolicy, NucliaDBRoles, Settings


@pytest.fixture()
def http_request():
    yield Mock(headers={}, query_params={})


def test_get_mapped_roles():
    settings = Settings(
        auth_policy_user_default_roles=[NucliaDBRoles.READER],
        auth_policy_role_mapping={
            "group": {"managers": [NucliaDBRoles.MANAGER]},
            "other": {"other": [NucliaDBRoles.READER]},
        },
    )

    assert set(
        auth.get_mapped_roles(settings=settings, data={"group": "managers"})
    ) == set(
        [
            NucliaDBRoles.READER.value,
            NucliaDBRoles.MANAGER.value,
        ]
    )

    # no values
    assert (
        auth.get_mapped_roles(
            settings=Settings(auth_policy_user_default_roles=[]), data={}
        )
        == []
    )


@pytest.mark.asyncio
async def test_auth_header_backend(http_request):
    backend = auth.get_auth_backend(
        Settings(
            auth_policy=AuthPolicy.UPSTREAM_AUTH_HEADER,
            auth_policy_user_header="X-User",
            auth_policy_user_default_roles=[NucliaDBRoles.READER],
        )
    )

    http_request.headers["X-User"] = "test"
    auth_creds, nuclia_user = await backend.authenticate(http_request)

    assert nuclia_user.display_name == "test"
    assert auth_creds.scopes == [NucliaDBRoles.READER.value]

    # no user header
    http_request.headers = {}
    assert await backend.authenticate(http_request) is None


@pytest.mark.asyncio
async def test_oauth2_backend(http_request):
    backend = auth.get_auth_backend(
        settings=Settings(
            auth_policy=AuthPolicy.UPSTREAM_OAUTH2,
            auth_policy_user_default_roles=[NucliaDBRoles.READER],
        )
    )

    http_request.headers[
        "Authorization"
    ] = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"  # noqa
    auth_creds, nuclia_user = await backend.authenticate(http_request)

    assert nuclia_user.display_name == "1234567890"
    assert auth_creds.scopes == [NucliaDBRoles.READER.value]

    # no auth header
    http_request.headers = {}
    assert await backend.authenticate(http_request) is None

    # bad auth header
    http_request.headers = {"Authorization": "lsdkf"}
    assert await backend.authenticate(http_request) is None

    # bad jwt header
    http_request.headers = {"Authorization": "Bearer lsdkf"}
    assert await backend.authenticate(http_request) is None

    http_request.headers = {"Authorization": "Bearer bad.jwt.token"}
    assert await backend.authenticate(http_request) is None


@pytest.mark.asyncio
async def test_basic_backend(http_request):
    backend = auth.get_auth_backend(
        settings=Settings(
            auth_policy=AuthPolicy.UPSTREAM_BASICAUTH,
            auth_policy_user_default_roles=[NucliaDBRoles.READER],
        )
    )

    http_request.headers["Authorization"] = "Basic am9obkBkb2UuY29tOmxzZGZramxrc2RqZmw="
    auth_creds, nuclia_user = await backend.authenticate(http_request)

    assert nuclia_user.display_name == "john@doe.com"
    assert auth_creds.scopes == [NucliaDBRoles.READER.value]

    # no auth header
    http_request.headers = {}
    assert await backend.authenticate(http_request) is None

    # bad auth header
    http_request.headers = {"Authorization": "lsdkf"}
    assert await backend.authenticate(http_request) is None


@pytest.mark.asyncio
async def test_auth_token_backend(http_request):
    jwk_key = orjson.dumps(jwk.JWK.generate(kty="oct", size=256, kid="dyn")).decode(
        "utf-8"
    )
    backend = auth.get_auth_backend(
        settings=Settings(
            auth_policy=AuthPolicy.UPSTREAM_BASICAUTH,
            auth_policy_user_default_roles=[NucliaDBRoles.READER],
            jwk_key=jwk_key,
        )
    )

    claims = {
        "iat": int(time.time()),
        "exp": int(time.time() + 30),
        "scopes": ["READER"],
        "username": "display_name",
    }
    payload = orjson.dumps(claims)
    jwetoken = jwe.JWE(payload, orjson.dumps({"alg": "A256KW", "enc": "A256CBC-HS512"}))
    jwetoken.add_recipient(jwk.JWK(**orjson.loads(jwk_key)))
    token = jwetoken.serialize(compact=True)

    http_request.query_params["eph-token"] = token
    auth_creds, nuclia_user = await backend.authenticate(http_request)

    assert nuclia_user.display_name == "display_name"
    assert auth_creds.scopes == [NucliaDBRoles.READER.value]
