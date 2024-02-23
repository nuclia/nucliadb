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

from io import BytesIO
from unittest import mock

import pytest
from fastapi import Response
from fastapi.responses import StreamingResponse

from nucliadb_models.resource import NucliaDBRoles


class MockProxy:
    def __init__(self):
        self.calls = []

    async def __call__(self, request, method, url, headers=None):
        self.calls.append((request, method, url, headers))
        if method == "GET" and "2021" in url:

            async def iter_content():
                yield b"some streamed content"

            return StreamingResponse(content=iter_content(), status_code=200)
        else:
            return Response(content=b"some content", status_code=200)


@pytest.fixture()
def learning_collector_proxy_mock():
    proxy = MockProxy()
    with mock.patch(
        "nucliadb.reader.api.v1.learning_collector.learning_collector_proxy", proxy
    ):
        yield proxy


async def test_api(reader_api, knowledgebox_ingest, learning_collector_proxy_mock):
    kbid = knowledgebox_ingest
    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        # Get feedback months
        resp = await client.get(f"/kb/{kbid}/learning/collect/feedback")
        assert resp.status_code == 200
        assert learning_collector_proxy_mock.calls[-1][1:] == (
            "GET",
            f"/collect/feedback/{kbid}",
            None,
        )

        # Download feedback for a month
        resp = await client.get(f"/kb/{kbid}/learning/collect/feedback/2021-01")
        assert resp.status_code == 200
        data = BytesIO()
        for chunk in resp.iter_bytes():
            data.write(chunk)
        assert data.getvalue() == b"some streamed content"
        assert learning_collector_proxy_mock.calls[-1][1:] == (
            "GET",
            f"/collect/feedback/{kbid}/2021-01",
            None,
        )
