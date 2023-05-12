from unittest.mock import Mock

import pytest

from nucliadb.standalone import auth
from nucliadb.standalone.settings import AuthPolicy, NucliaDBRoles, Settings


@pytest.fixture()
def http_request():
    yield Mock(headers={})


def test_get_mapped_roles():
    settings = Settings(
        auth_policy_user_deafult_roles=[NucliaDBRoles.READER],
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
            settings=Settings(auth_policy_user_deafult_roles=[]), data={}
        )
        == []
    )


@pytest.mark.asyncio
async def test_auth_header_backend(http_request):
    backend = auth.get_auth_backend(
        Settings(
            auth_policy=AuthPolicy.UPSTREAM_AUTH_HEADER,
            auth_policy_user_header="X-User",
            auth_policy_user_deafult_roles=[NucliaDBRoles.READER],
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
            auth_policy_user_deafult_roles=[NucliaDBRoles.READER],
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
async def test_baseic_backend(http_request):
    backend = auth.get_auth_backend(
        settings=Settings(
            auth_policy=AuthPolicy.UPSTREAM_BASICAUTH,
            auth_policy_user_deafult_roles=[NucliaDBRoles.READER],
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
