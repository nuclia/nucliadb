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

from nucliadb.http_clients import exceptions, processing
from nucliadb_utils.settings import nuclia_settings


def test_check_proxy_telemetry_headers_ok():
    resp = Response(
        headers={"x-b3-traceid": "foo", "x-b3-spanid": "bar", "x-b3-sampled": "baz"}
    )
    with mock.patch("nucliadb.http_clients.processing.logger") as logger:
        processing.check_proxy_telemetry_headers(resp)
        logger.warning.assert_not_called()


def test_check_proxy_telemetry_headers_error_logs():
    resp = Response(headers={})
    with mock.patch("nucliadb.http_clients.processing.logger") as logger:
        processing.check_proxy_telemetry_headers(resp)
        logger.warning.assert_called_once()


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
    with pytest.raises(exceptions.ClientException):
        processing.check_status(Response(status=500), "unk")


def test_get_processing_api_url():
    with mock.patch.object(
        nuclia_settings, "nuclia_service_account", "sa"
    ), mock.patch.object(
        nuclia_settings, "nuclia_zone", "nuclia_zone"
    ), mock.patch.object(
        nuclia_settings, "nuclia_public_url", "https://{zone}.nuclia_public_url"
    ):
        assert (
            processing.get_processing_api_url()
            == "https://nuclia_zone.nuclia_public_url/api/v1/processing"
        )

    with mock.patch.object(
        nuclia_settings, "nuclia_cluster_url", "https://nuclia_cluster_url"
    ):
        assert (
            processing.get_processing_api_url()
            == "https://nuclia_cluster_url/api/internal/processing"
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
        cl.session = mock.MagicMock()
        resp_handler = mock.AsyncMock()
        resp_handler.__aenter__.return_value = response
        cl.session.get.return_value = resp_handler
        yield cl

    @pytest.mark.asyncio
    async def test_status(self, client: processing.ProcessingHTTPClient, response):
        response_data = processing.StatusResponse(shared={"foobar": 54}, account={})
        response.status = 200
        response.text.return_value = response_data.json()

        assert await client.status() == response_data

    @pytest.mark.asyncio
    async def test_pull(self, client: processing.ProcessingHTTPClient, response):
        response_data = processing.PullResponse(status="ok", data="foobar")
        response.status = 200
        response.text.return_value = response_data.json()

        assert await client.pull(partition="1") == response_data
