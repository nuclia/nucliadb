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

import functools
from collections import OrderedDict
from concurrent import futures
from contextlib import contextmanager
from typing import Any, Awaitable, Callable, List, Optional, Tuple

import grpc
from grpc import ChannelCredentials, ClientCallDetails, aio
from grpc.experimental import wrap_server_method_handler
from opentelemetry.context import attach, detach
from opentelemetry.propagate import extract, inject
from opentelemetry.propagators.textmap import CarrierT, Setter  # type: ignore
from opentelemetry.sdk.trace import Span  # type: ignore
from opentelemetry.sdk.trace import TracerProvider  # type: ignore
from opentelemetry.semconv.trace import SpanAttributes  # type: ignore
from opentelemetry.trace import SpanKind  # type: ignore
from opentelemetry.trace import Tracer  # type: ignore
from opentelemetry.trace.status import Status, StatusCode  # type: ignore

from nucliadb_telemetry import grpc_metrics, logger
from nucliadb_telemetry.common import set_span_exception


class _CarrierSetter(Setter):
    """We use a custom setter in order to be able to lower case
    keys as is required by grpc.
    """

    def set(self, carrier: CarrierT, key: str, value: str):  # type: ignore
        carrier[key.lower()] = value  # type: ignore


_carrier_setter = _CarrierSetter()


def finish_span_grpc(span: Span, result):
    code = result._cython_call._status.code()
    if code != grpc.StatusCode.OK:
        span.set_status(
            Status(
                status_code=StatusCode.OK,
            )
        )
    else:
        span.set_status(
            Status(
                status_code=StatusCode.ERROR,
                description=result._cython_call._status.details(),
            )
        )
    span.end()


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
        SpanAttributes.RPC_GRPC_STATUS_CODE: grpc.StatusCode.OK.value[0],  # type: ignore
        SpanAttributes.RPC_METHOD: meth,
        SpanAttributes.RPC_SERVICE: service,
    }

    # add some attributes from the metadata
    if client_call_details.metadata is not None:
        mutable_metadata = OrderedDict(tuple(client_call_details.metadata))
        inject(mutable_metadata, setter=_carrier_setter)  # type: ignore
        for key, value in mutable_metadata.items():
            client_call_details.metadata.add(key=key, value=value)  # type: ignore

    span = tracer.start_span(  # type: ignore
        name=method_name,
        kind=SpanKind.CLIENT,
        attributes=attributes,  # type: ignore
        set_status_on_exception=set_status_on_exception,
    )
    return span


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

    def start_span_server(
        self,
        handler_call_details: grpc.HandlerCallDetails,
        context: grpc.ServicerContext,
        set_status_on_exception=False,
    ):
        service, meth = handler_call_details.method.lstrip("/").split("/", 1)  # type: ignore

        attributes = {
            SpanAttributes.RPC_SYSTEM: "grpc",
            SpanAttributes.RPC_GRPC_STATUS_CODE: grpc.StatusCode.OK.value[0],  # type: ignore
            SpanAttributes.RPC_METHOD: meth,
            SpanAttributes.RPC_SERVICE: service,
        }

        # add some attributes from the metadata
        metadata = dict(context.invocation_metadata())
        if "user-agent" in metadata:
            attributes["rpc.user_agent"] = metadata["user-agent"]

        # Split up the peer to keep with how other telemetry sources
        # do it.  This looks like:
        # * ipv6:[::1]:57284
        # * ipv4:127.0.0.1:57284
        # * ipv4:10.2.1.1:57284,127.0.0.1:57284
        #
        try:
            ip, port = context.peer().split(",")[0].split(":", 1)[1].rsplit(":", 1)
            attributes.update(
                {SpanAttributes.NET_PEER_IP: ip, SpanAttributes.NET_PEER_PORT: port}
            )

            # other telemetry sources add this, so we will too
            if ip in ("[::1]", "127.0.0.1"):
                attributes[SpanAttributes.NET_PEER_NAME] = "localhost"

        except IndexError:
            logger.warning("Failed to parse peer address '%s'", context.peer())

        return self.tracer.start_as_current_span(  # type: ignore
            name=handler_call_details.method,  # type: ignore
            kind=SpanKind.SERVER,
            attributes=attributes,
            set_status_on_exception=set_status_on_exception,
        )

    # Handle streaming responses separately - we have to do this
    # to return a *new* generator or various upstream things
    # get confused, or we'll lose the consistent trace
    async def _intercept_server_stream(
        self, behavior, handler_call_details, request_or_iterator, context
    ):
        with self._set_remote_context(context):
            with self.start_span_server(
                handler_call_details, context, set_status_on_exception=False
            ) as span:
                try:
                    async for response in behavior(request_or_iterator, context):
                        yield response

                except Exception as error:
                    # pylint:disable=unidiomatic-typecheck
                    if type(error) != Exception:
                        span.record_exception(error)
                    raise error

    @contextmanager
    def _set_remote_context(self, servicer_context):
        metadata = servicer_context.invocation_metadata()
        if metadata:
            md_dict = {md.key: md.value for md in metadata}
            ctx = extract(md_dict)
            token = attach(ctx)
            try:
                yield
            finally:
                detach(token)
        else:
            yield

    async def intercept_service(
        self,
        continuation: Callable[
            [grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]
        ],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        handler = await continuation(handler_call_details)
        if handler and (
            handler.request_streaming or handler.response_streaming
        ):  # pytype: disable=attribute-error
            return handler

        def wrapper(behavior: Callable[[Any, aio.ServicerContext], Any]):
            @functools.wraps(behavior)
            async def wrapper(request: Any, context: aio.ServicerContext) -> Any:
                with self._set_remote_context(context):
                    with self.start_span_server(
                        handler_call_details,
                        context,  # type: ignore
                        set_status_on_exception=False,
                    ) as span:
                        # And now we run the actual RPC.
                        try:
                            value = await behavior(request, context)

                        except Exception as error:
                            # Bare exceptions are likely to be gRPC aborts, which
                            # we handle in our context wrapper.
                            # Here, we're interested in uncaught exceptions.
                            # pylint:disable=unidiomatic-typecheck
                            if type(error) != Exception:
                                span.record_exception(error)
                            raise error
                return value

            return wrapper

        if "grpc.health.v1.Health" in handler_call_details.method:  # type: ignore
            return handler

        return wrap_server_method_handler(wrapper, handler)


class UnaryUnaryClientInterceptor(aio.UnaryUnaryClientInterceptor):
    """Interceptor used for testing if the interceptor is being called"""

    def __init__(self, tracer):
        self.tracer = tracer

    async def intercept_unary_unary(
        self,
        continuation,
        client_call_details: ClientCallDetails,  # type: ignore
        request,
    ):
        span = start_span_client(self.tracer, client_call_details)
        try:
            call = await continuation(client_call_details, request)
        except Exception as error:
            if type(error) != Exception:
                set_span_exception(span, error)
            raise error
        else:
            call.add_done_callback(functools.partial(finish_span_grpc, span))
        return call


class UnaryStreamClientInterceptor(aio.UnaryStreamClientInterceptor):
    """Interceptor used for testing if the interceptor is being called"""

    def __init__(self, tracer):
        self.tracer = tracer

    async def intercept_unary_stream(
        self,
        continuation,
        client_call_details: ClientCallDetails,  # type: ignore
        request,
    ):
        span = start_span_client(self.tracer, client_call_details)

        try:
            call = await continuation(client_call_details, request)
        except Exception as error:
            if type(error) != Exception:
                set_span_exception(span, error)
            raise error
        else:
            call.add_done_callback(functools.partial(finish_span_grpc, span))

        return call


class StreamStreamClientInterceptor(aio.StreamStreamClientInterceptor):
    """Interceptor used for testing if the interceptor is being called"""

    def __init__(self, tracer):
        self.tracer = tracer

    async def intercept_stream_stream(
        self,
        continuation,
        client_call_details: ClientCallDetails,  # type: ignore
        request_iterator,
    ):
        span = start_span_client(self.tracer, client_call_details)
        try:
            call = await continuation(client_call_details, request_iterator)
        except Exception as error:
            if type(error) != Exception:
                set_span_exception(span, error)
            raise error
        else:
            call.add_done_callback(functools.partial(finish_span_grpc, span))

        return call


class StreamUnaryClientInterceptor(aio.StreamUnaryClientInterceptor):
    """Interceptor used for testing if the interceptor is being called"""

    def __init__(self, tracer):
        self.tracer = tracer

    async def intercept_stream_unary(
        self,
        continuation,
        client_call_details: ClientCallDetails,  # type: ignore
        request_iterator,
    ):
        span = start_span_client(self.tracer, client_call_details)
        try:
            call = await continuation(client_call_details, request_iterator)
        except Exception as error:
            if type(error) != Exception:
                set_span_exception(span, error)
            raise error
        else:
            call.add_done_callback(functools.partial(finish_span_grpc, span))

        return call


def get_client_interceptors(service_name: str, tracer_provider: TracerProvider):
    tracer = tracer_provider.get_tracer(f"{service_name}_grpc_client")
    return [
        UnaryUnaryClientInterceptor(tracer),
        UnaryStreamClientInterceptor(tracer),
        StreamUnaryClientInterceptor(tracer),
        StreamStreamClientInterceptor(tracer),
    ]


def get_server_interceptors(service_name: str, tracer_provider: TracerProvider):
    tracer = tracer_provider.get_tracer(f"{service_name}_grpc_server")
    return [OpenTelemetryServerInterceptor(tracer)]


class GRPCTelemetry:
    initialized: bool = False

    def __init__(self, service_name: str, tracer_provider: TracerProvider):
        self.service_name = service_name
        self.tracer_provider = tracer_provider

    def init_client(
        self,
        server_addr: str,
        max_send_message: int = 100,
        credentials: Optional[ChannelCredentials] = None,
        options: Optional[List[Tuple[str, Any]]] = None,
    ):
        options = [
            ("grpc.max_receive_message_length", max_send_message * 1024 * 1024),
            ("grpc.max_send_message_length", max_send_message * 1024 * 1024),
        ] + (options or [])
        interceptors = (
            get_client_interceptors(self.service_name, self.tracer_provider)
            + grpc_metrics.CLIENT_INTERCEPTORS
        )
        if credentials is not None:
            channel = aio.secure_channel(
                server_addr,
                options=options,
                credentials=credentials,
                interceptors=interceptors,
            )
        else:
            channel = aio.insecure_channel(
                server_addr, options=options, interceptors=interceptors
            )
        return channel

    def init_server(
        self,
        concurrency: int = 4,
        max_receive_message: int = 100,
        interceptors: Optional[List[aio.ServerInterceptor]] = None,
        options: Optional[List[Tuple[str, Any]]] = None,
    ):
        _interceptors = (
            get_server_interceptors(self.service_name, self.tracer_provider)
            + grpc_metrics.SERVER_INTERCEPTORS
        )
        if interceptors is not None:
            _interceptors.extend(interceptors)
        options = [
            ("grpc.max_send_message_length", max_receive_message * 1024 * 1024),
            ("grpc.max_receive_message_length", max_receive_message * 1024 * 1024),
        ] + (options or [])
        server = aio.server(
            futures.ThreadPoolExecutor(max_workers=concurrency),
            interceptors=_interceptors,
            options=options,
        )
        return server


OpenTelemetryGRPC = GRPCTelemetry  # b/w compat import
