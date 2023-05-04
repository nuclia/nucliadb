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
from unittest import mock

import pytest

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.exceptions import LimitsExceededError


@pytest.fixture(scope="function")
def chat_with_limits_exceeded_error():
    with mock.patch(
        "nucliadb.search.api.v1.chat.chat",
        side_effect=LimitsExceededError(402, "over the quota"),
    ):
        yield


@pytest.mark.flaky(reruns=5)
@pytest.mark.asyncio()
async def test_chat_handles_limits_exceeded_error(
    search_api, knowledgebox_ingest, chat_with_limits_exceeded_error
):
    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        kb = knowledgebox_ingest
        resp = await client.post(f"/{KB_PREFIX}/{kb}/chat", json={})
        assert resp.status_code == 402
        assert resp.json() == {"detail": "over the quota"}
