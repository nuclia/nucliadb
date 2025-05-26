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

from datetime import datetime
from functools import partial
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union
from urllib.parse import ParseResult

import nats
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js.client import JetStreamContext
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import SpanKind, Tracer

from nucliadb_telemetry import logger, metrics
from nucliadb_telemetry.common import set_span_exception
from nucliadb_telemetry.utils import get_telemetry

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
    attributes: dict[str, Union[str, int]] = {
        SpanAttributes.MESSAGING_DESTINATION_KIND: "nats",
        SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.data),
        SpanAttributes.MESSAGING_MESSAGE_ID: msg.reply,
    }

    # add some attributes from the metadata
    ctx = extract(msg.headers or {})

    span = tracer.start_as_current_span(
        name=f"Received from {msg.subject}",
        context=ctx,
        kind=SpanKind.SERVER,
        attributes=attributes,
    )

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


async def _traced_callback(origin_cb: Callable[[Msg], Awaitable[None]], tracer: Tracer, msg: Msg):
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
                    "acked": "yes" if msg._ackd else "no",
                },
            )


class JetStreamContextTelemetry:
    def __init__(self, js: JetStreamContext, service_name: str, tracer_provider: TracerProvider):
        self.js = js
        self.service_name = service_name
        self.tracer_provider = tracer_provider

    async def stream_info(self, name: str):
        return await self.js.stream_info(name)

    async def add_stream(self, name: str, subjects: List[str]):
        return await self.js.add_stream(name=name, subjects=subjects)

    async def subscribe(
        self,
        subject: str,
        queue: Optional[str] = None,
        cb: Optional[Callable[[Msg], Awaitable[None]]] = None,
        **kwargs,
    ):
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_js_subscriber")
        wrapped_cb: Optional[Callable[[Msg], Awaitable[None]]]
        if cb is not None:
            wrapped_cb = partial(_traced_callback, cb, tracer)
        else:
            wrapped_cb = cb
        return await self.js.subscribe(subject, queue=queue, cb=wrapped_cb, **kwargs)

    async def publish(
        self,
        subject: str,
        body: bytes,
        headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_js_publisher")
        headers = {} if headers is None else headers
        with start_span_message_publisher(tracer, subject) as span:
            inject(headers)
            try:
                result = await self.js.publish(subject, body, headers=headers, **kwargs)
                msg_sent_counter.inc({"subject": subject, "status": metrics.OK})
            except Exception as error:
                set_span_exception(span, error)
                msg_sent_counter.inc({"subject": subject, "status": metrics.ERROR})
                raise error

        return result

    async def trace_pull_subscriber_message(self, cb: Callable[[Msg], Awaitable[None]], msg: Msg):
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_js_pull_subscriber")
        await _traced_callback(cb, tracer, msg)

    # Just for convenience, to wrap all we use in the context of
    # telemetry-instrumented stuff using the JetStreamContextTelemetry class

    async def pull_subscribe(
        self,
        subject: str,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[nats.js.api.ConsumerConfig] = None,
    ) -> JetStreamContext.PullSubscription:
        return await self.js.pull_subscribe(subject, durable=durable, stream=stream, config=config)  # type: ignore

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

    async def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb: Optional[Callable[[Msg], Awaitable[None]]] = None,
        **kwargs,
    ) -> Subscription:
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
        return await self.nc.subscribe(subject, queue=queue, cb=wrapped_cb, **kwargs)

    async def publish(
        self,
        subject: str,
        body: bytes = b"",
        reply: str = "",
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_nc_publisher")
        headers = {} if headers is None else headers

        with start_span_message_publisher(tracer, subject) as span:
            inject(headers)
            try:
                await self.nc.publish(subject, body, reply, headers)
                msg_sent_counter.inc({"subject": subject, "status": metrics.OK})
            except Exception as error:
                set_span_exception(span, error)
                msg_sent_counter.inc({"subject": subject, "status": metrics.ERROR})
                raise error

    async def request(
        self,
        subject: str,
        payload: bytes = b"",
        timeout: float = 0.5,
        old_style: bool = False,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Msg:
        headers = {} if headers is None else headers
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_nc_request")
        with start_span_message_publisher(tracer, subject) as span:
            inject(headers)
            try:
                result = await self.nc.request(subject, payload, timeout, old_style, headers)
            except Exception as error:
                set_span_exception(span, error)
                raise error

        return result

    # Other methods we use but don't need telemetry

    @property
    def is_connected(self) -> bool:
        return self.nc.is_connected

    @property
    def connected_url(self) -> Optional[ParseResult]:
        return self.nc.connected_url

    def jetstream(self, **opts) -> nats.js.JetStreamContext:
        return self.nc.jetstream(**opts)

    def jsm(self, **opts) -> nats.js.JetStreamManager:
        return self.nc.jsm(**opts)

    async def drain(self) -> None:
        return await self.nc.drain()

    async def flush(self, timeout: int = nats.aio.client.DEFAULT_FLUSH_TIMEOUT) -> None:
        return await self.nc.flush(timeout)

    async def close(self) -> None:
        return await self.nc.close()


def get_traced_nats_client(nc: Client, service_name: str) -> Union[Client, NatsClientTelemetry]:
    tracer_provider = get_telemetry(service_name)
    if tracer_provider is not None:
        return NatsClientTelemetry(nc, service_name, tracer_provider)
    else:
        return nc


def get_traced_jetstream(
    nc: Union[Client, NatsClientTelemetry], service_name: str
) -> Union[JetStreamContext, JetStreamContextTelemetry]:
    jetstream = nc.jetstream()
    tracer_provider = get_telemetry(service_name)

    if tracer_provider is not None and jetstream is not None:  # pragma: no cover
        logger.info(f"Configuring {service_name} jetstream with telemetry")
        return JetStreamContextTelemetry(jetstream, service_name, tracer_provider)
    else:
        return jetstream
