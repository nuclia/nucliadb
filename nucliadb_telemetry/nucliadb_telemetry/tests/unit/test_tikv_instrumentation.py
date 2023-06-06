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

from unittest.mock import AsyncMock, MagicMock, Mock, call

import pytest
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import TracerProvider
from tikv_client.asynchronous import TransactionClient  # type: ignore

from nucliadb_telemetry.tikv import TiKVInstrumentor


@pytest.mark.asyncio
async def test_tikv_instrumentation(
    mocked_tracer_provider: TracerProvider, mock_tikv_client
):
    tracer_provider = mocked_tracer_provider
    TiKVInstrumentor().instrument(tracer_provider=tracer_provider)

    tikv_url = "http://tikv-server-address"
    tikv_client = await TransactionClient.connect([tikv_url])

    tracer = tracer_provider.get_tracer("mock-tracer")
    with tracer.start_as_current_span(name="myspan"):  # type: ignore[attr-defined]
        txn = await tikv_client.begin(pessimistic=False)
        await txn.put(b"animal", b"cat")
        await txn.put(b"food", b"pizza")
        await txn.get(b"animal")

        with tracer.start_as_current_span(name="nested"):  # type: ignore[attr-defined]
            await txn.delete(b"food")
            await txn.commit()

    assert tracer.start_as_current_span.call_count == 8  # type: ignore[attr-defined]

    expected_calls = [
        call(name="myspan"),
        call(
            name="TiKV begin",
            attributes={
                SpanAttributes.DB_SYSTEM: "tikv",
                SpanAttributes.DB_OPERATION: "begin",
            },
        ),
        call(
            name="TiKV put",
            attributes={
                SpanAttributes.DB_SYSTEM: "tikv",
                SpanAttributes.DB_OPERATION: "put",
            },
        ),
        call(
            name="TiKV put",
            attributes={
                SpanAttributes.DB_SYSTEM: "tikv",
                SpanAttributes.DB_OPERATION: "put",
            },
        ),
        call(
            name="TiKV get",
            attributes={
                SpanAttributes.DB_SYSTEM: "tikv",
                SpanAttributes.DB_OPERATION: "get",
            },
        ),
        call(name="nested"),
        call(
            name="TiKV delete",
            attributes={
                SpanAttributes.DB_SYSTEM: "tikv",
                SpanAttributes.DB_OPERATION: "delete",
            },
        ),
        call(
            name="TiKV commit",
            attributes={
                SpanAttributes.DB_SYSTEM: "tikv",
                SpanAttributes.DB_OPERATION: "commit",
            },
        ),
    ]

    for i, mocked in enumerate(tracer.start_as_current_span.call_args_list):  # type: ignore[attr-defined]
        expected = expected_calls[i]
        assert mocked.kwargs["name"] == expected.kwargs["name"]

        expected_attrs = expected.kwargs.get("attributes")
        if expected_attrs:
            mocked_attrs = mocked.kwargs["attributes"]
            assert (
                mocked_attrs[SpanAttributes.DB_SYSTEM]
                == expected_attrs[SpanAttributes.DB_SYSTEM]
            )
            assert (
                mocked_attrs[SpanAttributes.DB_OPERATION]
                == expected_attrs[SpanAttributes.DB_OPERATION]
            )
            assert mocked_attrs[SpanAttributes.CODE_FILEPATH] == __file__
            assert __name__.endswith(mocked_attrs[SpanAttributes.CODE_FUNCTION])
            assert mocked_attrs[SpanAttributes.CODE_LINENO] > 0


@pytest.fixture
def mocked_tracer_provider() -> TracerProvider:
    span = Mock(name="span")
    span_ctx = MagicMock(name="span_context_manager")
    span_ctx.__enter__.return_value = span

    tracer = Mock(name="tracer")
    tracer.start_as_current_span.return_value = span_ctx

    tracer_provider = Mock(name="tracer_provider")
    tracer_provider.get_tracer = Mock(
        name="tracer_provider.get_tracer", return_value=tracer
    )
    return tracer_provider


@pytest.fixture
def mock_tikv_client():
    # HACK: mock TiKV client internals so we can test without need of running
    # TiKV server and simultaneously validate instrumentation, which
    # transparently wraps library methods
    txn_client = TransactionClient.__new__(TransactionClient)
    txn_client.inner = AsyncMock(return_value=AsyncMock())

    original = TransactionClient.connect
    TransactionClient.connect = AsyncMock(return_value=txn_client)

    yield

    TransactionClient.connect = original
