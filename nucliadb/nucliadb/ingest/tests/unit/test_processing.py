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
from unittest.mock import AsyncMock, Mock

import pytest

from nucliadb.ingest.processing import ProcessingEngine, PushPayload
from nucliadb_utils.exceptions import LimitsExceededError


@pytest.fixture(scope="function")
def proxy_session_402_error():
    session = AsyncMock()
    resp = Mock(status=402)
    resp.json = AsyncMock(return_value={"detail": "limits exceeded"})
    session.post.return_value = resp
    return session


@pytest.fixture(scope="function")
def proxy_session_413_error():
    session = AsyncMock()
    resp = Mock(status=413)
    resp.json = AsyncMock(return_value={"detail": "limits exceeded"})
    session.post.return_value = resp
    return session


@pytest.mark.asyncio
async def test_send_to_process_handles_proxy_limits_exceeded_error_responses(
    proxy_session_402_error, proxy_session_413_error
):
    pe = ProcessingEngine(
        onprem=True,
        nuclia_cluster_url="foo",
        nuclia_public_url="bar",
    )
    payload = PushPayload(uuid="uuid", kbid="kbid", userid="userid", partition=0)

    pe.session = proxy_session_402_error
    with pytest.raises(LimitsExceededError) as exc:
        await pe.send_to_process(payload, partition=0)
    assert exc.value.status_code == 402

    pe.session = proxy_session_413_error
    with pytest.raises(LimitsExceededError) as exc:
        await pe.send_to_process(payload, partition=0)
    assert exc.value.status_code == 413
