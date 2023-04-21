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
from unittest import mock

import pytest
from aiohttp.web import Response

from nucliadb.ingest.consumer.pull import check_proxy_telemetry_headers


@pytest.fixture(scope="function")
def errors():
    with mock.patch("nucliadb.ingest.consumer.pull.errors") as errors:
        yield errors


def test_check_proxy_telemetry_headers_ok(errors):
    resp = Response(
        headers={"x-b3-traceid": "foo", "x-b3-spanid": "bar", "x-b3-sampled": "baz"}
    )
    check_proxy_telemetry_headers(resp)

    errors.capture_exception.assert_not_called()
