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
import time
from types import SimpleNamespace

import aiohttp

from nucliadb_telemetry.metrics import INF, Counter, Gauge, Histogram

# How long a request had to wait for a free slot in the pool.
# Non-zero observations mean the pool is under pressure.
pool_wait_histogram = Histogram(
    "aiohttp_pool_wait_seconds",
    labels={"pool": ""},
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, INF],
)

# Total connections acquired, split by whether a new TCP connection was
# opened ("new") or an existing one was recycled ("reused").
pool_connections_counter = Counter(
    "aiohttp_pool_connections_total",
    labels={"pool": "", "type": ""},
)

# In-flight requests (incremented on request start, decremented on end/exception).
# A proxy for "connections currently in use" without touching connector internals.
pool_inflight_gauge = Gauge(
    "aiohttp_pool_inflight_requests",
    labels={"pool": ""},
)


def InstrumentedClientSession(
    pool_name: str,
    *,
    connector: aiohttp.TCPConnector | None = None,
    **session_kwargs,
) -> aiohttp.ClientSession:
    """Create an aiohttp.ClientSession instrumented with pool metrics.

    Drop-in replacement for aiohttp.ClientSession(). All keyword arguments
    are forwarded to the session unchanged.

    Args:
        pool_name: Short label used in all metric series, e.g. "predict".
        connector: Optional pre-built TCPConnector. A default one is created
                   if not supplied.
        **session_kwargs: Forwarded verbatim to aiohttp.ClientSession().
    """
    if connector is None:
        connector = aiohttp.TCPConnector()

    trace_config = _build_trace_config(pool_name)
    existing_configs = session_kwargs.pop("trace_configs", [])

    return aiohttp.ClientSession(
        connector=connector,
        trace_configs=[*existing_configs, trace_config],
        **session_kwargs,
    )


def _build_trace_config(pool_name: str) -> aiohttp.TraceConfig:
    trace_config = aiohttp.TraceConfig()

    async def on_connection_queued_start(
        session: aiohttp.ClientSession,
        ctx: SimpleNamespace,
        params: aiohttp.TraceConnectionQueuedStartParams,
    ) -> None:
        ctx._pool_wait_start = time.monotonic()  # type: ignore[attr-defined]

    async def on_connection_queued_end(
        session: aiohttp.ClientSession,
        ctx: SimpleNamespace,
        params: aiohttp.TraceConnectionQueuedEndParams,
    ) -> None:
        if ctx._pool_wait_start:
            pool_wait_histogram.observe(
                time.monotonic() - ctx._pool_wait_start,
                labels={"pool": pool_name},
            )

    async def on_connection_create_end(
        session: aiohttp.ClientSession,
        ctx: object,
        params: aiohttp.TraceConnectionCreateEndParams,
    ) -> None:
        pool_connections_counter.inc(labels={"pool": pool_name, "type": "new"})

    async def on_connection_reuseconn(
        session: aiohttp.ClientSession,
        ctx: object,
        params: aiohttp.TraceConnectionReuseconnParams,
    ) -> None:
        pool_connections_counter.inc(labels={"pool": pool_name, "type": "reused"})

    async def on_request_start(
        session: aiohttp.ClientSession,
        ctx: object,
        params: aiohttp.TraceRequestStartParams,
    ) -> None:
        pool_inflight_gauge.inc(1, labels={"pool": pool_name})

    async def on_request_end(
        session: aiohttp.ClientSession,
        ctx: object,
        params: aiohttp.TraceRequestEndParams,
    ) -> None:
        pool_inflight_gauge.dec(1, labels={"pool": pool_name})

    async def on_request_exception(
        session: aiohttp.ClientSession,
        ctx: object,
        params: aiohttp.TraceRequestExceptionParams,
    ) -> None:
        pool_inflight_gauge.dec(1, labels={"pool": pool_name})

    trace_config.on_connection_queued_start.append(on_connection_queued_start)
    trace_config.on_connection_queued_end.append(on_connection_queued_end)
    trace_config.on_connection_create_end.append(on_connection_create_end)
    trace_config.on_connection_reuseconn.append(on_connection_reuseconn)
    trace_config.on_request_start.append(on_request_start)
    trace_config.on_request_end.append(on_request_end)
    trace_config.on_request_exception.append(on_request_exception)

    return trace_config
