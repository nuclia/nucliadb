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
from opentelemetry.trace import format_trace_id

from nucliadb_telemetry.fastapi.tracing import CaptureTraceIdMiddleware


@pytest.fixture(scope="function")
def trace_id():
    tid = 123
    context = mock.Mock()
    context.trace_id = tid
    current_span = mock.Mock()
    current_span.get_span_context = mock.Mock(return_value=context)
    with mock.patch(
        "nucliadb_telemetry.fastapi.tracing.trace.get_current_span",
        return_value=current_span,
    ):
        yield tid


async def test_capture_trace_id_middleware(trace_id):
    request = mock.Mock()
    response = mock.Mock(headers={})
    call_next = mock.AsyncMock(return_value=response)

    mdw = CaptureTraceIdMiddleware(mock.Mock())
    response = await mdw.dispatch(request, call_next)

    assert response.headers["X-NUCLIA-TRACE-ID"] == format_trace_id(trace_id)
    assert response.headers["Access-Control-Expose-Headers"] == "X-NUCLIA-TRACE-ID"


async def test_capture_trace_id_middleware_appends_trace_id_header_to_exposed(trace_id):
    request = mock.Mock()
    response = mock.Mock(headers={"Access-Control-Expose-Headers": "Foo-Bar,X-Header"})
    call_next = mock.AsyncMock(return_value=response)

    mdw = CaptureTraceIdMiddleware(mock.Mock())
    response = await mdw.dispatch(request, call_next)

    assert (
        response.headers["Access-Control-Expose-Headers"]
        == "Foo-Bar,X-Header,X-NUCLIA-TRACE-ID"
    )
