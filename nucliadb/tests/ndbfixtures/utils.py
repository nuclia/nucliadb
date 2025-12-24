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
from collections.abc import Callable
from contextlib import contextmanager
from enum import Enum
from typing import Any
from unittest.mock import patch

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from nucliadb.search import API_PREFIX
from nucliadb_utils.utilities import MAIN

logger = logging.getLogger("fixtures.utils")


def create_api_client_factory(application: FastAPI) -> Callable[..., AsyncClient]:
    def _make_client_fixture(
        roles: list[Enum] | None = None,
        user: str = "",
        version: str = "1",
        root: bool = False,
        extra_headers: dict[str, str] | None = None,
    ) -> AsyncClient:
        roles = roles or []
        client_base_url = "http://test"

        if root is False:
            client_base_url = f"{client_base_url}/{API_PREFIX}/v{version}"

        transport = ASGITransport(app=application)  # type: ignore
        client = AsyncClient(transport=transport, base_url=client_base_url)
        client.headers["X-NUCLIADB-ROLES"] = ";".join([role.value for role in roles])
        client.headers["X-NUCLIADB-USER"] = user

        extra_headers = extra_headers or {}
        if len(extra_headers) == 0:
            return client

        for header, value in extra_headers.items():
            client.headers[f"{header}"] = value

        return client

    return _make_client_fixture


@contextmanager
def global_utility(name: str, util: Any):
    """Hacky set_utility used in tests to provide proper setup/cleanup of utilities.

    Tests can sometimes mess with global state. While fixtures add/remove global
    utilities, component lifecycles do the same. Sometimes, we can left
    utilities unclean or overwrite utilities. This context manager allows tests
    to remove utilities letting the previously set one.

    """

    if name in MAIN:
        logger.warning(f"Overwriting previously set utility {name}: {MAIN[name]} with {util}")

    with patch.dict(MAIN, values={name: util}, clear=False):
        yield util
