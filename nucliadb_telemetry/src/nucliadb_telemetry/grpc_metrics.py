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

import functools
from typing import Any, Awaitable, Callable, Union

import grpc
from grpc import ClientCallDetails, aio
from grpc.experimental import wrap_server_method_handler

from nucliadb_telemetry import metrics

histo_buckets = [
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    metrics.INF,
]
grpc_client_observer = metrics.Observer("grpc_client_op", labels={"method": ""}, buckets=histo_buckets)
grpc_server_observer = metrics.Observer("grpc_server_op", labels={"method": ""}, buckets=histo_buckets)


class MetricsServerInterceptor(aio.ServerInterceptor):
    """
    A GRPC server interceptor that adds metrics to the server.
    """

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
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
                with grpc_server_observer(labels={"method": handler_call_details.method}) as observer:
                    value = await behavior(request, context)
                    observer.set_status(str(context.code()))
                return value

            return wrapper

        return wrap_server_method_handler(wrapper, handler)


def finish_metric_grpc(metric: metrics.ObserverRecorder, result):
    code = result._cython_call._status.code()
    metric.set_status(str(code))
    metric.end()


def _to_str(v: Union[str, bytes]) -> str:
    if isinstance(v, str):
        return v
    return v.decode("utf-8")


class UnaryUnaryClientInterceptor(aio.UnaryUnaryClientInterceptor):
    async def intercept_unary_unary(
        self,
        continuation,
        client_call_details: ClientCallDetails,  # type: ignore
        request,
    ):
        metric = grpc_client_observer(labels={"method": _to_str(client_call_details.method)})
        metric.start()

        call = await continuation(client_call_details, request)
        call.add_done_callback(functools.partial(finish_metric_grpc, metric))

        return call


class UnaryStreamClientInterceptor(aio.UnaryStreamClientInterceptor):
    async def intercept_unary_stream(
        self,
        continuation,
        client_call_details: ClientCallDetails,  # type: ignore
        request,
    ):
        metric = grpc_client_observer(labels={"method": _to_str(client_call_details.method)})
        metric.start()

        call = await continuation(client_call_details, request)
        call.add_done_callback(functools.partial(finish_metric_grpc, metric))

        return call


class StreamStreamClientInterceptor(aio.StreamStreamClientInterceptor):
    async def intercept_stream_stream(
        self,
        continuation,
        client_call_details: ClientCallDetails,  # type: ignore
        request_iterator,
    ):
        metric = grpc_client_observer(labels={"method": _to_str(client_call_details.method)})
        metric.start()

        call = await continuation(client_call_details, request_iterator)
        call.add_done_callback(functools.partial(finish_metric_grpc, metric))

        return call


class StreamUnaryClientInterceptor(aio.StreamUnaryClientInterceptor):
    async def intercept_stream_unary(
        self,
        continuation,
        client_call_details: ClientCallDetails,  # type: ignore
        request_iterator,
    ):
        metric = grpc_client_observer(labels={"method": _to_str(client_call_details.method)})
        metric.start()

        call = await continuation(client_call_details, request_iterator)
        call.add_done_callback(functools.partial(finish_metric_grpc, metric))

        return call


CLIENT_INTERCEPTORS = [
    UnaryUnaryClientInterceptor(),
    UnaryStreamClientInterceptor(),
    StreamStreamClientInterceptor(),
    StreamUnaryClientInterceptor(),
]
SERVER_INTERCEPTORS = [MetricsServerInterceptor()]
