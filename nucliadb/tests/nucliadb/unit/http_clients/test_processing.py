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
from aiohttp.web import Response

from nucliadb.common.http_clients import exceptions, processing
from nucliadb_utils.settings import nuclia_settings


def test_check_status():
    processing.check_status(Response(status=200), "ok")
    with pytest.raises(exceptions.AccountLimitException):
        processing.check_status(Response(status=402), "account limit")
    with pytest.raises(exceptions.AuthorizationException):
        processing.check_status(Response(status=403), "auth")
    with pytest.raises(exceptions.NotFoundException):
        processing.check_status(Response(status=404), "notfound")
    with pytest.raises(exceptions.RateLimitException):
        processing.check_status(Response(status=429), "rate")
    with pytest.raises(exceptions.ServiceUnavailableException):
        processing.check_status(Response(status=503), "service unavailable")
    with pytest.raises(exceptions.ClientException):
        processing.check_status(Response(status=500), "unk")


def test_get_processing_api_url():
    with (
        mock.patch.object(nuclia_settings, "nuclia_service_account", "sa"),
        mock.patch.object(nuclia_settings, "nuclia_zone", "nuclia_zone"),
        mock.patch.object(nuclia_settings, "nuclia_public_url", "https://{zone}.nuclia_public_url"),
    ):
        assert (
            processing.get_processing_api_url()
            == "https://nuclia_zone.nuclia_public_url/api/v1/processing"
        )

    with mock.patch.object(
        nuclia_settings,
        "nuclia_processing_cluster_url",
        "https://nuclia_processing_cluster_url",
    ):
        assert (
            processing.get_processing_api_url()
            == "https://nuclia_processing_cluster_url/api/v1/internal/processing"
        )


class TestProcessingHTTPClient:
    @pytest.fixture()
    def response(self):
        resp = mock.AsyncMock()
        resp.status = 555
        resp.text.return_value = "notset"
        yield resp

    @pytest.fixture()
    async def client(self, response):
        cl = processing.ProcessingHTTPClient()
        cl.session = mock.MagicMock(close=mock.AsyncMock())
        resp_handler = mock.AsyncMock()
        resp_handler.__aenter__.return_value = response
        cl.session.get.return_value = resp_handler
        cl.session.post.return_value = resp_handler
        yield cl

    async def test_requests(self, client: processing.ProcessingHTTPClient, response):
        response_data = processing.RequestsResults(results=[], cursor=None)
        response.status = 200
        response.text.return_value = response_data.model_dump_json()

        assert await client.requests() == response_data

    async def test_stats(self, client: processing.ProcessingHTTPClient, response):
        response_data = processing.StatsResponse(incomplete=1, scheduled=1)
        response.status = 200
        response.text.return_value = response_data.model_dump_json()

        assert await client.stats("kbid") == response_data

    async def test_pull_status(self, client: processing.ProcessingHTTPClient, response):
        response_data = processing.PullStatusResponse(pending=1)
        response.status = 200
        response.text.return_value = response_data.model_dump_json()

        assert await client.pull_status() == response_data

    async def test_pull_v2(self, client: processing.ProcessingHTTPClient, response):
        response_data = processing.PullResponseV2(
            messages=[
                processing.PulledMessage(
                    payload=b"data",
                    headers={},
                    ack_token="ack",
                    seq=1,
                )
            ],
            ttl=10.0,
            pending=1,
        )
        response.status = 200
        response.text.return_value = response_data.model_dump_json()

        assert await client.pull_v2(ack_tokens=["ack"]) == response_data

        response.status = 204
        assert await client.pull_v2(ack_tokens=["ack"]) is None

    async def test_reset_session(self, client: processing.ProcessingHTTPClient):
        old_session = client.session
        await client.reset_session()
        assert client.session != old_session
        old_session.close.assert_awaited_once()  # type: ignore
        await client.close()
