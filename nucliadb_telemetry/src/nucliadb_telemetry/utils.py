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

import asyncio
import os
from typing import cast

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

import nucliadb_telemetry.batch_span
from nucliadb_telemetry.jaeger import JaegerExporterAsync
from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.tracerprovider import (
    AsyncMultiSpanProcessor,
    AsyncTracerProvider,
)

from .context import set_info_on_span

set_info_on_span  # b/w compatible import

GLOBAL_PROVIDER: dict[str, AsyncTracerProvider | TracerProvider] = {}


def get_telemetry(service_name: str | None = None) -> AsyncTracerProvider | TracerProvider | None:
    if service_name is None:
        return None
    if service_name not in GLOBAL_PROVIDER and service_name is not None:
        provider = create_telemetry(service_name)

        if provider is not None:
            GLOBAL_PROVIDER[service_name] = provider
    return GLOBAL_PROVIDER.get(service_name)


def create_telemetry(service_name: str) -> AsyncTracerProvider | TracerProvider | None:
    if not telemetry_settings.tracing_enabled():
        return None

    resource = Resource.create({SERVICE_NAME: service_name})
    if telemetry_settings.otlp_collector_endpoint is not None:
        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.initialized = False  # type: ignore
    else:
        tracer_provider = AsyncTracerProvider(
            active_span_processor=AsyncMultiSpanProcessor(),  # type: ignore
            resource=resource,
        )

    return tracer_provider


async def clean_telemetry(service_name: str):
    if service_name in GLOBAL_PROVIDER and service_name:
        tracer_provider = GLOBAL_PROVIDER[service_name]

        if isinstance(tracer_provider, AsyncTracerProvider):
            await tracer_provider.async_force_flush()
            # Without this sleep, async_force_flush fails on exporting pending spans
            await asyncio.sleep(0)
        else:
            tracer_provider.force_flush()

        tracer_provider.shutdown()
        del GLOBAL_PROVIDER[service_name]


async def init_telemetry(tracer_provider: AsyncTracerProvider | TracerProvider | None = None):
    if tracer_provider is None:
        return

    if tracer_provider.initialized:  # type: ignore
        return

    if telemetry_settings.otlp_collector_endpoint is not None:
        # Prefer to create a OTLP exporter if configured
        exporter = OTLPSpanExporter(endpoint=telemetry_settings.otlp_collector_endpoint, insecure=True)
        span_processor = BatchSpanProcessor(exporter)
        tracer_provider.add_span_processor(span_processor)
    else:
        # Fallback to create a JaegerExporter
        jaeger_exporter = JaegerExporterAsync(
            # configure agent
            agent_host_name=telemetry_settings.jaeger_agent_host,
            agent_port=telemetry_settings.jaeger_agent_port,
            # optional: configure also collector
            # collector_endpoint='http://localhost:14268/api/traces?format=jaeger.thrift',
            # username=xxxx, # optional
            # password=xxxx, # optional
            # max_tag_value_length=None # optional
        )
        schedule_delay_millis = int(os.environ.get("OTEL_BSP_SCHEDULE_DELAY", 5000))
        max_queue_size = int(os.environ.get("OTEL_BSP_MAX_QUEUE_SIZE", 2048))
        max_export_batch_size = int(os.environ.get("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", 512))
        export_timeout_millis = int(os.environ.get("OTEL_BSP_EXPORT_TIMEOUT", 30000))
        jaeger_span_processor = nucliadb_telemetry.batch_span.BatchSpanProcessor(
            jaeger_exporter,
            schedule_delay_millis=schedule_delay_millis,
            max_queue_size=max_queue_size,
            max_export_batch_size=max_export_batch_size,
            export_timeout_millis=export_timeout_millis,
        )
        tracer_provider = cast(AsyncTracerProvider, tracer_provider)
        await tracer_provider.async_add_span_processor(jaeger_span_processor)

    tracer_provider.initialized = True  # type: ignore


async def setup_telemetry(service_name: str) -> AsyncTracerProvider | TracerProvider | None:
    """
    Setup telemetry for a service if it is enabled
    """
    tracer_provider = get_telemetry(service_name)
    if tracer_provider is not None:  # pragma: no cover
        await init_telemetry(tracer_provider)
        set_global_textmap(B3MultiFormat())

        try:
            from opentelemetry.instrumentation.aiohttp_client import (
                AioHttpClientInstrumentor,
            )
            from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

            AioHttpClientInstrumentor().instrument(tracer_provider=tracer_provider)
            HTTPXClientInstrumentor().instrument(tracer_provider=tracer_provider)

        except ImportError:  # pragma: no cover
            pass
    return tracer_provider
