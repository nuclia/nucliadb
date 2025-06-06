# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from unittest import mock

import pytest
from opentelemetry.trace import format_trace_id

from nucliadb_telemetry.fastapi.tracing import (
    CaptureTraceIdMiddleware,
    collect_custom_request_headers_attributes,
)


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

    assert response.headers["Access-Control-Expose-Headers"] == "Foo-Bar,X-Header,X-NUCLIA-TRACE-ID"


def test_collect_custom_request_headers_attributes():
    scope = {"headers": [[b"x-filename", b"Synth\xe8ses\\3229-navigation.pdf"]]}
    collect_custom_request_headers_attributes(scope)
