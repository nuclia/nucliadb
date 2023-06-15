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

import sys

import tikv_client  # type: ignore
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor  # type: ignore
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind, Tracer
from wrapt import wrap_function_wrapper  # type: ignore

from nucliadb_telemetry.common import set_span_exception

_instruments = ("tikv-client ~= 0.0.3",)


def _instrument(
    tracer: Tracer,
):
    async def _traced_async_wrapper(func, instance, args, kwargs):
        operation = func.__name__
        attributes = {
            SpanAttributes.DB_SYSTEM: "tikv",
            SpanAttributes.DB_OPERATION: operation,
        }

        # Frame 0 is this wrapper call, frame 1 is it's caller
        sysframe = sys._getframe(1)
        attributes.update(
            {
                SpanAttributes.CODE_FILEPATH: sysframe.f_code.co_filename,
                SpanAttributes.CODE_LINENO: sysframe.f_lineno,
                SpanAttributes.CODE_FUNCTION: sysframe.f_code.co_name,
            }
        )

        span_name = f"TiKV {operation}"
        with tracer.start_as_current_span(
            name=span_name,
            kind=SpanKind.CLIENT,
            attributes=attributes,
        ) as span:
            try:
                result = await func(*args, **kwargs)
            except Exception as error:
                set_span_exception(span, error)
                raise error
            else:
                return result

    wrap_function_wrapper(
        "tikv_client.asynchronous", "TransactionClient.begin", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.get", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.get_for_update", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.key_exists", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.batch_get", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous",
        "Transaction.batch_get_for_update",
        _traced_async_wrapper,
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.scan", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.scan_keys", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.lock_keys", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.put", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.insert", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.delete", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.commit", _traced_async_wrapper
    )
    wrap_function_wrapper(
        "tikv_client.asynchronous", "Transaction.rollback", _traced_async_wrapper
    )


class TiKVInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self):
        return _instruments

    def _instrument(self, **kwargs):
        tracer_provider = kwargs.get("tracer_provider")
        tracer = trace.get_tracer("tikv_client", tracer_provider=tracer_provider)
        _instrument(tracer)

    def _uninstrument(self, **kwargs):
        unwrap(tikv_client.asynchronous.TransactionClient, "begin")
        unwrap(tikv_client.asynchronous.TransactionClient, "get")
        unwrap(tikv_client.asynchronous.TransactionClient, "get_for_update")
        unwrap(tikv_client.asynchronous.TransactionClient, "key_exists")
        unwrap(tikv_client.asynchronous.TransactionClient, "batch_get")
        unwrap(tikv_client.asynchronous.TransactionClient, "batch_get_for_update")
        unwrap(tikv_client.asynchronous.TransactionClient, "scan")
        unwrap(tikv_client.asynchronous.TransactionClient, "scan_keys")
        unwrap(tikv_client.asynchronous.TransactionClient, "lock_keys")
        unwrap(tikv_client.asynchronous.TransactionClient, "put")
        unwrap(tikv_client.asynchronous.TransactionClient, "insert")
        unwrap(tikv_client.asynchronous.TransactionClient, "delete")
        unwrap(tikv_client.asynchronous.TransactionClient, "commit")
        unwrap(tikv_client.asynchronous.TransactionClient, "rollback")
