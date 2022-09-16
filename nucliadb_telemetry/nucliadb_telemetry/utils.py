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

import asyncio
import os
from typing import Dict, Optional, Sequence, Union

from opentelemetry.sdk.resources import SERVICE_NAME  # type: ignore
from opentelemetry.sdk.resources import Resource  # type: ignore
from opentelemetry.trace import get_current_span

from nucliadb_telemetry.batch_span import BatchSpanProcessor
from nucliadb_telemetry.jaeger import JaegerExporterAsync
from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.tracerprovider import (
    AsyncMultiSpanProcessor,
    AsyncTracerProvider,
)

GLOBAL_PROVIDER: Dict[str, AsyncTracerProvider] = {}


def get_telemetry(service_name: Optional[str] = None) -> Optional[AsyncTracerProvider]:
    if service_name is None:
        return None
    if service_name not in GLOBAL_PROVIDER and service_name is not None:
        provider = create_telemetry(service_name)

        if provider is not None:
            GLOBAL_PROVIDER[service_name] = provider
    return GLOBAL_PROVIDER.get(service_name)


def create_telemetry(service_name: str) -> Optional[AsyncTracerProvider]:
    if telemetry_settings.jaeger_enabled is False:
        return None

    tracer_provider = AsyncTracerProvider(
        active_span_processor=AsyncMultiSpanProcessor(),
        resource=Resource.create({SERVICE_NAME: service_name}),
    )

    return tracer_provider


async def clean_telemetry(service_name: str):
    if service_name in GLOBAL_PROVIDER and service_name:
        tracer_provider = GLOBAL_PROVIDER[service_name]
        await tracer_provider.force_flush()
        # Without this sleep, force_flush fails on exporting pending spans
        await asyncio.sleep(0)
        tracer_provider.shutdown()
        del GLOBAL_PROVIDER[service_name]


async def init_telemetry(tracer_provider: Optional[AsyncTracerProvider] = None):
    if tracer_provider is None:
        return

    if tracer_provider.initialized:
        return

    # create a JaegerExporter
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

    # Create a BatchSpanProcessor and add the exporter to it
    schedule_delay_millis = int(os.environ.get("OTEL_BSP_SCHEDULE_DELAY", 5000))
    max_queue_size = int(os.environ.get("OTEL_BSP_MAX_QUEUE_SIZE", 2048))
    max_export_batch_size = int(os.environ.get("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", 512))
    export_timeout_millis = int(os.environ.get("OTEL_BSP_EXPORT_TIMEOUT", 30000))

    span_processor = BatchSpanProcessor(
        jaeger_exporter,
        schedule_delay_millis=schedule_delay_millis,
        max_queue_size=max_queue_size,
        max_export_batch_size=max_export_batch_size,
        export_timeout_millis=export_timeout_millis,
    )

    # add to the tracer
    await tracer_provider.add_span_processor(span_processor)
    tracer_provider.initialized = True


def set_info_on_span(
    headers: Dict[
        str,
        Union[
            str,
            bool,
            int,
            float,
            Sequence[str],
            Sequence[bool],
            Sequence[int],
            Sequence[float],
        ],
    ]
):
    if telemetry_settings.jaeger_enabled:
        span = get_current_span()
        span.set_attributes(headers)
