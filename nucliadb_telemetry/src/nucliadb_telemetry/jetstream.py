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

from datetime import datetime
from functools import partial
from typing import Any, Callable, Dict, List, Optional

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.js.client import JetStreamContext
from opentelemetry.context import attach
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind, Tracer

from nucliadb_telemetry import logger, metrics
from nucliadb_telemetry.common import set_span_exception

msg_consume_time_histo = metrics.Histogram(
    # time it takes from when msg was queue to when it finished processing
    "nuclia_nats_msg_op_time",
    labels={
        "stream": "",
        "consumer": "",
        "acked": "no",
    },
    buckets=[
        0.005,
        0.025,
        0.05,
        0.1,
        0.5,
        1.0,
        5.0,
        10.0,
        30.0,
        60.0,
        120.0,
        600.0,
        metrics.INF,
    ],
)

msg_sent_counter = metrics.Counter("nuclia_nats_msg_sent", labels={"subject": "", "status": metrics.OK})


def start_span_message_receiver(tracer: Tracer, msg: Msg):
    attributes = {
        SpanAttributes.MESSAGING_DESTINATION_KIND: "nats",
        SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.data),
        SpanAttributes.MESSAGING_MESSAGE_ID: msg.reply,
    }

    # add some attributes from the metadata
    ctx = extract(msg.headers)
    token = attach(ctx)

    span = tracer.start_as_current_span(  # type: ignore
        name=f"Received from {msg.subject}",
        kind=SpanKind.SERVER,
        attributes=attributes,
    )
    span._token = token
    return span


def start_span_message_publisher(tracer: Tracer, subject: str):
    attributes = {
        SpanAttributes.MESSAGING_DESTINATION_KIND: "nats",
        SpanAttributes.MESSAGING_DESTINATION: subject,
    }

    span = tracer.start_as_current_span(  # type: ignore
        name=f"Published on {subject}",
        kind=SpanKind.CLIENT,
        attributes=attributes,
    )
    return span


class JetStreamContextTelemetry:
    def __init__(self, js: JetStreamContext, service_name: str, tracer_provider: TracerProvider):
        self.js = js
        self.service_name = service_name
        self.tracer_provider = tracer_provider

    async def stream_info(self, name: str):
        return await self.js.stream_info(name)

    async def add_stream(self, name: str, subjects: List[str]):
        return await self.js.add_stream(name=name, subjects=subjects)

    async def subscribe(self, cb, **kwargs):
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_js_subscriber")

        async def wrapper(origin_cb, tracer, msg: Msg):
            # Execute the callback without tracing
            if msg.headers is None:
                logger.debug("Message received without headers, skipping span")
                await origin_cb(msg)
                return

            with start_span_message_receiver(tracer, msg) as span:
                try:
                    await origin_cb(msg)
                except Exception as error:
                    set_span_exception(span, error)
                    raise error
                finally:
                    msg_consume_time_histo.observe(
                        (datetime.now() - msg.metadata.timestamp).total_seconds(),
                        {
                            "stream": msg.metadata.stream,
                            "consumer": msg.metadata.consumer or "",
                            "acked": "yes" if msg._ackd else "no",  # type: ignore
                        },
                    )

        wrapped_cb = partial(wrapper, cb, tracer)
        return await self.js.subscribe(cb=wrapped_cb, **kwargs)

    async def publish(
        self,
        subject: str,
        body: bytes,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_js_publisher")
        headers = {} if headers is None else headers
        inject(headers)
        with start_span_message_publisher(tracer, subject) as span:
            try:
                result = await self.js.publish(subject, body, headers=headers, **kwargs)
                msg_sent_counter.inc({"subject": subject, "status": metrics.OK})
            except Exception as error:
                set_span_exception(span, error)
                msg_sent_counter.inc({"subject": subject, "status": metrics.ERROR})
                raise error

        return result

    # Just for convenience, to wrap all we use in the context of
    # telemetry-instrumented stuff using the JetStreamContextTelemetry class

    async def pull_subscribe(self, *args, **kwargs) -> JetStreamContext.PullSubscription:
        return await self.js.pull_subscribe(*args, **kwargs)

    async def pull_subscribe_bind(self, *args, **kwargs) -> JetStreamContext.PullSubscription:
        return await self.js.pull_subscribe_bind(*args, **kwargs)

    async def pull_one(
        self,
        subscription: JetStreamContext.PullSubscription,
        cb: Callable[[Msg], Any],
        timeout: int = 5,
    ) -> Msg:
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_js_pull_one")
        messages = await subscription.fetch(1, timeout=timeout)

        # If there is no message, fetch will raise a timeout
        message = messages[0]

        # Execute the callback without tracing
        if message.headers is None:
            logger.debug("Message received without headers, skipping span")
            return await cb(message)

        with start_span_message_receiver(tracer, message) as span:
            try:
                return await cb(message)
            except Exception as error:
                set_span_exception(span, error)
                raise error
            finally:
                msg_consume_time_histo.observe(
                    (datetime.now() - message.metadata.timestamp).total_seconds(),
                    {
                        "stream": message.metadata.stream,
                        "consumer": message.metadata.consumer or "",
                        "acked": "yes" if message._ackd else "no",
                    },
                )

    async def consumer_info(self, stream: str, consumer: str, timeout: Optional[float] = None):
        return await self.js.consumer_info(stream, consumer, timeout)


class NatsClientTelemetry:
    def __init__(self, nc: Client, service_name: str, tracer_provider: TracerProvider):
        self.nc = nc
        self.service_name = service_name
        self.tracer_provider = tracer_provider

    async def subscribe(self, cb, **kwargs):
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_nc_subscriber")

        async def wrapper(origin_cb, tracer, msg: Msg):
            # Execute the callback without tracing
            if msg.headers is None:
                logger.debug("Message received without headers, skipping span")
                await origin_cb(msg)
                return

            with start_span_message_receiver(tracer, msg) as span:
                try:
                    await origin_cb(msg)
                except Exception as error:
                    set_span_exception(span, error)
                    raise error

        wrapped_cb = partial(wrapper, cb, tracer)
        return await self.nc.subscribe(cb=wrapped_cb, **kwargs)

    async def publish(
        self,
        subject: str,
        body: bytes,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_nc_publisher")
        headers = {} if headers is None else headers
        inject(headers)

        with start_span_message_publisher(tracer, subject) as span:
            try:
                result = await self.nc.publish(subject, body, headers=headers, **kwargs)
                msg_sent_counter.inc({"subject": subject, "status": metrics.OK})
            except Exception as error:
                set_span_exception(span, error)
                msg_sent_counter.inc({"subject": subject, "status": metrics.ERROR})
                raise error

        return result

    async def request(
        self,
        subject: str,
        payload: bytes = b"",
        timeout: float = 0.5,
        old_style: bool = False,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Msg:
        headers = {} if headers is None else headers
        inject(headers)
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_nc_request")
        with start_span_message_publisher(tracer, subject) as span:
            try:
                result = await self.nc.request(subject, payload, timeout, old_style, headers)
            except Exception as error:
                set_span_exception(span, error)
                raise error

        return result
