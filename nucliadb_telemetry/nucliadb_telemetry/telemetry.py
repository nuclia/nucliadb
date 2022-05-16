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
#
import asyncio
import math
import traceback
from collections import OrderedDict
from concurrent import futures
from functools import partial
from typing import Awaitable, Callable, Dict, List, MutableMapping, Optional

import grpc
from grpc import ClientCallDetails, aio  # type: ignore
from nats.aio.msg import Msg
from nats.js.client import JetStreamContext
from opentelemetry.context import (  # type: ignore
    _SUPPRESS_INSTRUMENTATION_KEY,
    Context,
    attach,
    detach,
    set_value,
)
from opentelemetry.exporter.jaeger.thrift import JaegerExporter  # type: ignore
from opentelemetry.exporter.jaeger.thrift.gen.agent import Agent  # type: ignore
from opentelemetry.exporter.jaeger.thrift.gen.jaeger import Collector  # type: ignore
from opentelemetry.exporter.jaeger.thrift.translate import Translate  # type: ignore
from opentelemetry.exporter.jaeger.thrift.translate import (  # type: ignore
    ThriftTranslator,
)
from opentelemetry.propagate import extract, inject
from opentelemetry.propagators.textmap import Setter  # type: ignore
from opentelemetry.sdk.resources import SERVICE_NAME  # type: ignore
from opentelemetry.sdk.resources import Resource  # type: ignore
from opentelemetry.sdk.trace import TracerProvider  # type: ignore
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor  # type: ignore
from opentelemetry.sdk.trace.export import (  # type: ignore
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.semconv.trace import SpanAttributes  # type: ignore
from opentelemetry.trace import SpanKind  # type: ignore
from opentelemetry.trace import Tracer  # type: ignore
from opentelemetry.trace.status import Status, StatusCode  # type: ignore
from opentelemetry.util._time import _time_ns  # type: ignore
from thrift.protocol import TCompactProtocol  # type: ignore
from thrift.transport import TTransport  # type: ignore

from nucliadb_telemetry import logger
from nucliadb_telemetry.settings import telemetry_settings

UDP_PACKET_MAX_LENGTH = 65000


class _CarrierSetter(Setter):
    """We use a custom setter in order to be able to lower case
    keys as is required by grpc.
    """

    def set(self, carrier: MutableMapping[str, str], key: str, value: str):
        carrier[key.lower()] = value


_carrier_setter = _CarrierSetter()


def init_telemetry(service_name: str):
    if telemetry_settings.jeager_enabled is False:
        return

    tracer_provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name})
    )

    # create a JaegerExporter
    jaeger_exporter = JaegerExporterAsync(
        # configure agent
        agent_host_name=telemetry_settings.jaeger_host,
        agent_port=telemetry_settings.jaeger_port,
        # optional: configure also collector
        # collector_endpoint='http://localhost:14268/api/traces?format=jaeger.thrift',
        # username=xxxx, # optional
        # password=xxxx, # optional
        # max_tag_value_length=None # optional
    )

    # Create a BatchSpanProcessor and add the exporter to it
    span_processor = BatchSpanProcessor(jaeger_exporter)

    # add to the tracer
    tracer_provider.add_span_processor(span_processor)
    return tracer_provider


def start_span_server(
    tracer: Tracer,
    handler_call_details: grpc.HandlerCallDetails,
    set_status_on_exception=False,
):

    service, meth = handler_call_details.method.lstrip("/").split("/", 1)
    attributes = {
        SpanAttributes.RPC_SYSTEM: "grpc",
        SpanAttributes.RPC_GRPC_STATUS_CODE: grpc.StatusCode.OK.value,
        SpanAttributes.RPC_METHOD: meth,
        SpanAttributes.RPC_SERVICE: service,
    }

    # add some attributes from the metadata
    metadata = dict(handler_call_details.invocation_metadata)
    ctx = extract(metadata)
    token = attach(ctx)

    if "user-agent" in metadata:
        attributes["rpc.user_agent"] = metadata["user-agent"]

    span = tracer.start_as_current_span(  # type: ignore
        name=handler_call_details.method,
        kind=SpanKind.SERVER,
        attributes=attributes,
        set_status_on_exception=set_status_on_exception,
    )
    span._token = token
    return span


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
        name=subject,
        kind=SpanKind.CLIENT,
        attributes=attributes,
    )
    return span


def start_span_client(
    tracer: Tracer,
    client_call_details: grpc.ClientCallDetails,
    set_status_on_exception=False,
):

    if isinstance(client_call_details.method, bytes):
        service, meth = client_call_details.method.decode().lstrip("/").split("/", 1)
        method_name = client_call_details.method.decode()
    else:
        service, meth = client_call_details.method.lstrip("/").split("/", 1)
        method_name = client_call_details.method

    attributes = {
        SpanAttributes.RPC_SYSTEM: "grpc",
        SpanAttributes.RPC_GRPC_STATUS_CODE: grpc.StatusCode.OK.value,
        SpanAttributes.RPC_METHOD: meth,
        SpanAttributes.RPC_SERVICE: service,
    }

    # add some attributes from the metadata
    if client_call_details.metadata is not None:
        mutable_metadata = OrderedDict(client_call_details.metadata)
        inject(mutable_metadata, setter=_carrier_setter)
        for key, value in mutable_metadata.items():
            client_call_details.metadata.add(key=key, value=value)  # type: ignore

    span = tracer.start_as_current_span(  # type: ignore
        name=method_name,
        kind=SpanKind.CLIENT,
        attributes=attributes,
        set_status_on_exception=set_status_on_exception,
    )
    return span


def set_span_exception(span: Optional[Span], exception: Exception):
    if span is not None:
        description = traceback.format_exc()

        span.set_status(
            Status(
                status_code=StatusCode.ERROR,
                description=description,
            )
        )
        span.record_exception(exception)
        span.end()


def finish_span(span: Optional[Span]):
    if span is not None:
        span.end()
    if hasattr(span, "token"):
        detach(span.token)


class _FlushRequest:
    """Represents a request for the BatchSpanProcessor to flush spans."""

    __slots__ = ["event", "num_spans"]

    def __init__(self):
        self.event = asyncio.Event()
        self.num_spans = 0


class BatchSpanProcessor(SpanProcessor):
    """Batch span processor implementation.
    `BatchSpanProcessor` is an implementation of `SpanProcessor` that
    batches ended spans and pushes them to the configured `SpanExporter`.
    `BatchSpanProcessor` is configurable with the following environment
    variables which correspond to constructor parameters:
    - :envvar:`OTEL_BSP_SCHEDULE_DELAY`
    - :envvar:`OTEL_BSP_MAX_QUEUE_SIZE`
    - :envvar:`OTEL_BSP_MAX_EXPORT_BATCH_SIZE`
    - :envvar:`OTEL_BSP_EXPORT_TIMEOUT`
    """

    def __init__(
        self,
        span_exporter: SpanExporter,
        max_queue_size: int = 2048,
        schedule_delay_millis: int = 5000,
        max_export_batch_size: int = 512,
        export_timeout_millis: float = 30000,
    ):

        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer.")

        if schedule_delay_millis <= 0:
            raise ValueError("schedule_delay_millis must be positive.")

        if max_export_batch_size <= 0:
            raise ValueError("max_export_batch_size must be a positive integer.")

        if max_export_batch_size > max_queue_size:
            raise ValueError(
                "max_export_batch_size must be less than or equal to max_queue_size."
            )

        self.span_exporter = span_exporter
        self.queue = asyncio.Queue(maxsize=max_queue_size)  # type: asyncio.Queue[Span]
        self.worker_task: asyncio.Task = asyncio.create_task(
            self.worker(), name="OtelBatchSpanProcessor"
        )
        self.condition = asyncio.Condition()
        self._flush_request = None  # type: Optional[_FlushRequest]
        self.schedule_delay_millis = schedule_delay_millis
        self.max_export_batch_size = max_export_batch_size
        self.max_queue_size = max_queue_size
        self.export_timeout_millis = export_timeout_millis
        self.done = False
        # flag that indicates that spans are being dropped
        self._spans_dropped = False
        # precallocated list to send spans to exporter
        self.spans_list = [
            None
        ] * self.max_export_batch_size  # type: List[Optional[Span]]

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        if self.done:
            logger.warning("Already shutdown, dropping span.")
            return
        if not span.context.trace_flags.sampled:
            return
        if self.queue.qsize() == self.max_queue_size:
            if not self._spans_dropped:
                logger.warning("Queue is full, likely spans will be dropped.")
                self._spans_dropped = True

        self.queue.put_nowait(span)

        if self.queue.qsize() >= self.max_export_batch_size:
            asyncio.create_task(self.notify())

    async def notify(self):
        async with self.condition:
            self.condition.notify()

    async def notify_all(self):
        async with self.condition:
            self.condition.notify_all()

    async def worker(self):
        timeout = self.schedule_delay_millis / 1e3
        flush_request = None  # type: Optional[_FlushRequest]
        while not self.done:
            async with self.condition:
                if self.done:
                    # done flag may have changed, avoid waiting
                    break
                flush_request = self._get_and_unset_flush_request()
                if (
                    self.queue.qsize() < self.max_export_batch_size
                    and flush_request is None
                ):
                    try:
                        await asyncio.wait_for(self.condition.wait(), timeout)
                    except asyncio.TimeoutError:
                        pass
                    flush_request = self._get_and_unset_flush_request()
                    if not self.queue:
                        # spurious notification, let's wait again, reset timeout
                        timeout = self.schedule_delay_millis / 1e3
                        self._notify_flush_request_finished(flush_request)
                        flush_request = None
                        continue
                    if self.done:
                        # missing spans will be sent when calling flush
                        break

            # subtract the duration of this export call to the next timeout
            start = _time_ns()
            await self._export(flush_request)
            end = _time_ns()
            duration = (end - start) / 1e9
            timeout = self.schedule_delay_millis / 1e3 - duration

            self._notify_flush_request_finished(flush_request)
            flush_request = None

        # there might have been a new flush request while export was running
        # and before the done flag switched to true
        with self.condition:
            shutdown_flush_request = self._get_and_unset_flush_request()

        # be sure that all spans are sent
        self._drain_queue()
        self._notify_flush_request_finished(flush_request)
        self._notify_flush_request_finished(shutdown_flush_request)

    def _get_and_unset_flush_request(
        self,
    ) -> Optional[_FlushRequest]:
        """Returns the current flush request and makes it invisible to the
        worker thread for subsequent calls.
        """
        flush_request = self._flush_request
        self._flush_request = None
        if flush_request is not None:
            flush_request.num_spans = self.queue.qsize()
        return flush_request

    @staticmethod
    def _notify_flush_request_finished(
        flush_request: Optional[_FlushRequest],
    ):
        """Notifies the flush initiator(s) waiting on the given request/event
        that the flush operation was finished.
        """
        if flush_request is not None:
            flush_request.event.set()

    def _get_or_create_flush_request(self) -> _FlushRequest:
        """Either returns the current active flush event or creates a new one.
        The flush event will be visible and read by the worker thread before an
        export operation starts. Callers of a flush operation may wait on the
        returned event to be notified when the flush/export operation was
        finished.
        This method is not thread-safe, i.e. callers need to take care about
        synchronization/locking.
        """
        if self._flush_request is None:
            self._flush_request = _FlushRequest()
        return self._flush_request

    async def _export(self, flush_request: Optional[_FlushRequest]):
        """Exports spans considering the given flush_request.
        In case of a given flush_requests spans are exported in batches until
        the number of exported spans reached or exceeded the number of spans in
        the flush request.
        In no flush_request was given at most max_export_batch_size spans are
        exported.
        """
        if not flush_request:
            await self._export_batch()
            return

        num_spans = flush_request.num_spans
        while self.queue.qsize():
            num_exported = await self._export_batch()
            num_spans -= num_exported

            if num_spans <= 0:
                break

    async def _export_batch(self) -> int:
        """Exports at most max_export_batch_size spans and returns the number of
        exported spans.
        """
        idx = 0
        # currently only a single thread acts as consumer, so queue.pop() will
        # not raise an exception
        while idx < self.max_export_batch_size and self.queue.qsize():
            self.spans_list[idx] = self.queue.get_nowait()
            idx += 1
        token = attach(set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        try:
            # Ignore type b/c the Optional[None]+slicing is too "clever"
            # for mypy
            await self.span_exporter.export(self.spans_list[:idx])  # type: ignore
        except Exception:  # pylint: disable=broad-except
            logger.exception("Exception while exporting Span batch.")
        detach(token)

        # clean up list
        for index in range(idx):
            self.spans_list[index] = None
        return idx

    async def _drain_queue(self):
        """Export all elements until queue is empty.
        Can only be called from the worker thread context because it invokes
        `export` that is not thread safe.
        """
        while self.queue.qsize():
            await self._export_batch()

    # async def force_flush(self, timeout_millis: int = None) -> bool:

    #     if timeout_millis is None:
    #         timeout_millis = self.export_timeout_millis

    #     if self.done:
    #         logger.warning("Already shutdown, ignoring call to force_flush().")
    #         return True

    #     async with self.condition:
    #         flush_request = self._get_or_create_flush_request()
    #         # signal the worker thread to flush and wait for it to finish
    #         await self.condition.notify_all()

    #     # wait for token to be processed
    #     ret = flush_request.event.wait(timeout_millis / 1e3)
    #     if not ret:
    #         logger.warning("Timeout was exceeded in force_flush().")
    #     return ret

    def shutdown(self) -> None:
        # signal the worker thread to finish and then wait for it
        self.done = True
        asyncio.create_task(self.notify_all())
        self.span_exporter.shutdown()


class JaegerExporterAsync(JaegerExporter):
    def __init__(self, **kwags):
        super(JaegerExporterAsync, self).__init__(**kwags)
        self._agent_client = AgentClientUDPAsync(
            host_name=self.agent_host_name,
            port=self.agent_port,
            split_oversized_batches=self.udp_split_oversized_batches,
        )

    async def export(self, spans: List[Span]) -> SpanExportResult:
        # Populate service_name from first span
        # We restrict any SpanProcessor to be only associated with a single
        # TracerProvider, so it is safe to assume that all Spans in a single
        # batch all originate from one TracerProvider (and in turn have all
        # the same service.name)
        if spans:
            service_name = spans[0].resource.attributes.get(SERVICE_NAME)
            if service_name:
                self.service_name = service_name
        translator = Translate(spans)
        thrift_translator = ThriftTranslator(self._max_tag_value_length)
        jaeger_spans = translator._translate(thrift_translator)
        batch = Collector.Batch(
            spans=jaeger_spans,
            process=Collector.Process(serviceName=self.service_name),
        )
        if self._collector_http_client is not None:
            raise Exception("Not supported on asyncio")
            # self._collector_http_client.submit(batch)
        else:
            await self._agent_client.emit(batch)

        return SpanExportResult.SUCCESS


class JaegerClientProtocol:
    def __init__(self, message, on_con_lost):
        self.message = message
        self.on_con_lost = on_con_lost
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        print("Send:", self.message)
        self.transport.sendto(self.message)


class AgentClientUDPAsync:
    """Implement a UDP client to agent.

    Args:
        host_name: The host name of the Jaeger server.
        port: The port of the Jaeger server.
        max_packet_size: Maximum size of UDP packet.
        client: Class for creating new client objects for agencies.
        split_oversized_batches: Re-emit oversized batches in smaller chunks.
    """

    def __init__(
        self,
        host_name,
        port,
        max_packet_size=UDP_PACKET_MAX_LENGTH,
        client=Agent.Client,
        split_oversized_batches=False,
    ):
        self.host_name = host_name
        self.port = port
        self.max_packet_size = max_packet_size
        self.buffer = TTransport.TMemoryBuffer()
        self.client = client(iprot=TCompactProtocol.TCompactProtocol(trans=self.buffer))
        self.split_oversized_batches = split_oversized_batches

    async def emit(self, batch: Collector.Batch):
        """
        Args:
            batch: Object to emit Jaeger spans.
        """

        # pylint: disable=protected-access
        self.client._seqid = 0
        #  truncate and reset the position of BytesIO object
        self.buffer._buffer.truncate(0)
        self.buffer._buffer.seek(0)
        self.client.emitBatch(batch)
        buff = self.buffer.getvalue()
        if len(buff) > self.max_packet_size:
            if self.split_oversized_batches and len(batch.spans) > 1:
                packets = math.ceil(len(buff) / self.max_packet_size)
                div = math.ceil(len(batch.spans) / packets)
                for packet in range(packets):
                    start = packet * div
                    end = (packet + 1) * div
                    if start < len(batch.spans):
                        await self.emit(
                            Collector.Batch(
                                process=batch.process,
                                spans=batch.spans[start:end],
                            )
                        )
            else:
                logger.warning(
                    "Data exceeds the max UDP packet size; size %r, max %r",
                    len(buff),
                    self.max_packet_size,
                )
            return

        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: JaegerClientProtocol(buff, on_con_lost),  # type: ignore
            remote_addr=(self.host_name, self.port),
        )
        try:
            await on_con_lost
        finally:
            transport.close()


class OpenTelemetryServerInterceptor(aio.ServerInterceptor):
    """
    A gRPC server interceptor, to add OpenTelemetry.
    Usage::
        tracer = some OpenTelemetry tracer
        interceptors = [
            OpenTelemetryServerInterceptor(tracer),
        ]
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=concurrency),
            interceptors = interceptors)
    """

    def __init__(self, tracer):
        self.tracer = tracer

    async def intercept_service(
        self,
        continuation: Callable[
            [grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]
        ],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:

        with start_span_server(self.tracer, handler_call_details) as span:
            try:
                result = await continuation(handler_call_details)
            except Exception as error:
                if type(error) != Exception:
                    set_span_exception(span, error)
                raise error
            else:
                finish_span(span)
        return result


class UnaryUnaryClientInterceptor(aio.UnaryUnaryClientInterceptor):
    """Interceptor used for testing if the interceptor is being called"""

    def __init__(self, tracer):
        self.tracer = tracer

    async def intercept_unary_unary(
        self, continuation, client_call_details: ClientCallDetails, request
    ):
        with start_span_client(self.tracer, client_call_details) as span:
            try:
                call = await continuation(client_call_details, request)
            except Exception as error:
                if type(error) != Exception:
                    set_span_exception(span, error)
                raise error
            else:
                finish_span(span)
        return call


class UnaryStreamClientInterceptor(aio.UnaryStreamClientInterceptor):
    """Interceptor used for testing if the interceptor is being called"""

    def __init__(self, tracer):
        self.tracer = tracer

    async def intercept_unary_stream(
        self, continuation, client_call_details: ClientCallDetails, request
    ):
        with start_span_client(self.tracer, client_call_details) as span:
            try:
                call = await continuation(client_call_details, request)
            except Exception as error:
                if type(error) != Exception:
                    set_span_exception(span, error)
                raise error
            else:
                finish_span(span)
        return call


class StreamStreamClientInterceptor(aio.StreamStreamClientInterceptor):
    """Interceptor used for testing if the interceptor is being called"""

    def __init__(self, tracer):
        self.tracer = tracer

    async def intercept_stream_stream(
        self, continuation, client_call_details: ClientCallDetails, request_iterator
    ):
        with start_span_client(self.tracer, client_call_details) as span:
            try:
                call = await continuation(client_call_details, request_iterator)
            except Exception as error:
                if type(error) != Exception:
                    set_span_exception(span, error)
                raise error
            else:
                finish_span(span)
        return call


class StreamUnaryClientInterceptor(aio.StreamUnaryClientInterceptor):
    """Interceptor used for testing if the interceptor is being called"""

    def __init__(self, tracer):
        self.tracer = tracer

    async def intercept_stream_unary(
        self, continuation, client_call_details: ClientCallDetails, request_iterator
    ):
        with start_span_client(self.tracer, client_call_details) as span:
            try:
                call = await continuation(client_call_details, request_iterator)
            except Exception as error:
                if type(error) != Exception:
                    set_span_exception(span, error)
                raise error
            else:
                finish_span(span)
        return call


class OpenTelemetryGRPC:
    initialized: bool = False

    def __init__(self, service_name: str, tracer_provider: TracerProvider):
        self.service_name = service_name
        self.tracer_provider = tracer_provider

    def init_client(self, server_addr: str):
        tracer = self.tracer_provider.get_tracer(self.service_name)
        channel = aio.insecure_channel(
            server_addr,
            interceptors=[
                UnaryUnaryClientInterceptor(tracer=tracer),
                UnaryStreamClientInterceptor(tracer=tracer),
                StreamStreamClientInterceptor(tracer=tracer),
                StreamUnaryClientInterceptor(tracer=tracer),
            ],
        )
        return channel

    def init_server(self, concurrency: int = 4):
        tracer = self.tracer_provider.get_tracer(self.service_name)
        interceptors = [OpenTelemetryServerInterceptor(tracer=tracer)]
        server = aio.server(
            futures.ThreadPoolExecutor(max_workers=concurrency),
            interceptors=interceptors,
        )
        return server


class JetStreamContextTelemetry:
    def __init__(
        self, js: JetStreamContext, service_name: str, tracer_provider: TracerProvider
    ):
        self.js = js
        self.service_name = service_name
        self.tracer_provider = tracer_provider

    async def subscribe(self, cb, **kwargs):
        tracer = self.tracer_provider.get_tracer(self.service_name)

        async def wrapper(origin_cb, tracer, msg: Msg):
            with start_span_server_js(tracer, msg) as span:
                try:
                    await origin_cb(msg)
                except Exception as error:
                    if type(error) != Exception:
                        set_span_exception(span, error)
                    raise error
                else:
                    finish_span(span)

        wrapped_cb = partial(wrapper, cb, tracer)
        return await self.js.subscribe(cb=wrapped_cb, **kwargs)

    async def publish(self, subject: str, body: bytes):
        tracer = self.tracer_provider.get_tracer(self.service_name)
        headers: Dict[str, str] = {}
        inject(headers)
        with start_span_client_js(tracer, subject) as span:
            try:
                result = await self.js.publish(subject, body, headers=headers)
            except Exception as error:
                if type(error) != Exception:
                    set_span_exception(span, error)
                raise error
            else:
                finish_span(span)

        return result
