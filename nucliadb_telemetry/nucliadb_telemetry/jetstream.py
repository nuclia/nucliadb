from functools import partial
from typing import Dict, List

from nats.aio.msg import Msg
from nats.js.client import JetStreamContext
from opentelemetry.context import attach
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.trace import TracerProvider  # type: ignore
from opentelemetry.semconv.trace import SpanAttributes  # type: ignore
from opentelemetry.trace import SpanKind  # type: ignore
from opentelemetry.trace import Tracer  # type: ignore

from nucliadb_telemetry.common import set_span_exception


def start_span_server_js(tracer: Tracer, msg: Msg):

    attributes = {
        SpanAttributes.MESSAGING_DESTINATION_KIND: "nats",
        SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES: len(msg.data),
        SpanAttributes.MESSAGING_MESSAGE_ID: msg.reply,
    }

    # add some attributes from the metadata
    ctx = extract(msg.headers)
    token = attach(ctx)

    span = tracer.start_as_current_span(  # type: ignore
        name=msg.reply,
        kind=SpanKind.SERVER,
        attributes=attributes,
    )
    span._token = token
    return span


def start_span_client_js(tracer: Tracer, subject: str):

    attributes = {
        SpanAttributes.MESSAGING_DESTINATION_KIND: "nats",
        SpanAttributes.MESSAGING_DESTINATION: subject,
    }

    span = tracer.start_as_current_span(  # type: ignore
        name=f"Publish on {subject}",
        kind=SpanKind.CLIENT,
        attributes=attributes,
    )
    return span


class JetStreamContextTelemetry:
    def __init__(
        self, js: JetStreamContext, service_name: str, tracer_provider: TracerProvider
    ):
        self.js = js
        self.service_name = service_name
        self.tracer_provider = tracer_provider

    async def stream_info(self, name: str):
        return self.js.stream_info(name)

    async def add_stream(self, name: str, subjects: List[str]):
        return self.js.add_stream(name=name, subjects=subjects)

    async def subscribe(self, cb, **kwargs):
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_js_server")

        async def wrapper(origin_cb, tracer, msg: Msg):
            with start_span_server_js(tracer, msg) as span:
                try:
                    await origin_cb(msg)
                except Exception as error:
                    if type(error) != Exception:
                        set_span_exception(span, error)
                    raise error

        wrapped_cb = partial(wrapper, cb, tracer)
        return await self.js.subscribe(cb=wrapped_cb, **kwargs)

    async def publish(self, subject: str, body: bytes):
        tracer = self.tracer_provider.get_tracer(f"{self.service_name}_js_client")
        headers: Dict[str, str] = {}
        inject(headers)
        with start_span_client_js(tracer, subject) as span:
            try:
                result = await self.js.publish(subject, body, headers=headers)
            except Exception as error:
                if type(error) != Exception:
                    set_span_exception(span, error)
                raise error

        return result
