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
from typing import Optional

import pydantic

from nucliadb.ingest.settings import DriverSettings
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.settings import StorageSettings


class StandaloneDiscoveryMode(str, Enum):
    DEFAULT = "default"
    MANUAL = "manual"
    KUBERNETES = "kubernetes"
    SINGLE_NODE = "single_node"


class LogLevel(str, Enum):
    INFO = "INFO"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class AuthPolicy(str, Enum):
    UPSTREAM_NAIVE = "upstream_naive"
    UPSTREAM_AUTH_HEADER = "upstream_auth_header"
    UPSTREAM_OAUTH2 = "upstream_oauth2"
    UPSTREAM_BASICAUTH = "upstream_basicauth"


class Settings(DriverSettings, StorageSettings):
    # be consistent here with DATA_PATH env var
    data_path: str = pydantic.Field(
        "./data/node", description="Path to node index files"
    )

    # all settings here are mapped in to other env var settings used
    # in the app. These are helper settings to make things easier to
    # use with standalone app vs cluster app.
    nua_api_key: Optional[str] = pydantic.Field(
        description="Nuclia Understanding API Key. Read how to generate a NUA Key here: https://docs.nuclia.dev/docs/docs/using/understanding/intro#get-a-nua-key"  # noqa
    )
    zone: Optional[str] = pydantic.Field(description="Nuclia Understanding API Zone ID")
    http_port: int = pydantic.Field(8080, description="HTTP Port")
    ingest_grpc_port: int = pydantic.Field(8030, description="Ingest GRPC Port")
    train_grpc_port: int = pydantic.Field(8031, description="Train GRPC Port")
    standalone_node_port: int = pydantic.Field(10009, description="Node GRPC Port")

    auth_policy: AuthPolicy = pydantic.Field(
        AuthPolicy.UPSTREAM_NAIVE,
        description="""Auth policy to use for http requests.
- `upstream_naive` will assume `X-NUCLIADB-ROLES` and `X-NUCLIADB-USER` http headers are
   set by a trusted upstream proxy. This can also be used for testing locally with no auth
   proxy, manually supplying headers.
- `upstream_auth_header` will assume request is validated upstream and upstream passes header
   defined in `auth_policy_header` setting.
- `upstream_oauth2` will assume Bearer token is validated upstream and is passed down in `Authorization` header.
- `upstream_basicauth` will assume Basic Auth is validated upstream and is passed down in `Authorization` header.
""",
    )
    auth_policy_user_header: str = pydantic.Field(
        "X-NUCLIADB-USER",
        description="Header to read user id from. Only used for \
                    `upstream_naive` and `upstream_auth_header` auth policy.",
    )
    auth_policy_roles_header: str = pydantic.Field(
        "X-NUCLIADB-ROLES", description="Only used for `upstream_naive` auth policy."
    )
    auth_policy_user_default_roles: list[NucliaDBRoles] = pydantic.Field(
        [NucliaDBRoles.READER, NucliaDBRoles.WRITER, NucliaDBRoles.MANAGER],
        description="Default role to assign to user that is authenticated \
                    upstream. Not used with `upstream_naive` auth policy.",
    )
    auth_policy_role_mapping: Optional[
        dict[str, dict[str, list[NucliaDBRoles]]]
    ] = pydantic.Field(
        None,
        description="""
Role mapping for `upstream_auth_header`, `upstream_oauth2` and `upstream_basicauth` auth policies.
Allows mapping different properties from the auth request to a role.
Available roles are: `READER`, `WRITER`, `MANAGER`.
Examples:
- `{"user": {"john@doe.com": ["READER", "WRITER"]}}` will map the user `john@doe.com`
  to the role `MANAGER` on `upstream_auth_header` policies.
- `{"group": {"managers": "MANAGER"}}` will map the users that have a `group` claim of
  `managers` on the jwt provided by upstream to the role `MANAGER` on `upstream_oauth2` policies.
""",
    )

    jwk_key: Optional[str] = pydantic.Field(
        None, description="JWK key used for temporary token generation and validation."
    )

    cluster_discovery_mode: StandaloneDiscoveryMode = StandaloneDiscoveryMode.DEFAULT

    fork: bool = pydantic.Field(default=False, description="Fork process on startup")
