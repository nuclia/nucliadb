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

from unittest.mock import patch

import pytest
from grpc.aio import AioRpcError

from nucliadb_telemetry.grpc import GRPCTelemetry
from nucliadb_telemetry.grpc_sentry import SentryInterceptor
from nucliadb_telemetry.tests.grpc import (
    hellostreamingworld_pb2,
    hellostreamingworld_pb2_grpc,
    helloworld_pb2,
    helloworld_pb2_grpc,
)


class UnhanldedError(Exception):
    pass


class SimpleGreeter(
    helloworld_pb2_grpc.GreeterServicer,
    hellostreamingworld_pb2_grpc.MultiGreeterServicer,
):
    async def SayHello(self, request, context):
        return helloworld_pb2.HelloReply(message=f"Hello, {request.name}!")

    async def sayHello(self, request, context):
        for _ in range(10):
            yield hellostreamingworld_pb2.HelloReply(message=f"Hello, {request.name}!")


class FaultyGreeter(
    helloworld_pb2_grpc.GreeterServicer,
    hellostreamingworld_pb2_grpc.MultiGreeterServicer,
):
    async def SayHello(self, request, context):
        raise UnhanldedError("Unhandled error at the server")

    async def sayHello(self, request, context):
        yield hellostreamingworld_pb2.HelloReply(message=f"Hello, {request.name}!")
        raise UnhanldedError("Unhandled error at the server")


async def run_service(telemetry_grpc: GRPCTelemetry, greeter):
    server = telemetry_grpc.init_server(interceptors=[SentryInterceptor()])
    helloworld_pb2_grpc.add_GreeterServicer_to_server(greeter, server)
    hellostreamingworld_pb2_grpc.add_MultiGreeterServicer_to_server(greeter, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    return server, port


@pytest.fixture(scope="function")
async def grpc_service(telemetry_grpc: GRPCTelemetry):
    greeter = SimpleGreeter()
    server, port = await run_service(telemetry_grpc, greeter)
    yield port
    await server.stop(grace=True)


@pytest.fixture(scope="function")
async def faulty_grpc_service(telemetry_grpc: GRPCTelemetry):
    greeter = FaultyGreeter()
    server, port = await run_service(telemetry_grpc, greeter)
    yield port
    await server.stop(grace=True)


async def test_sentry_interceptor_without_errors(telemetry_grpc: GRPCTelemetry, grpc_service: int):
    port = grpc_service
    channel = telemetry_grpc.init_client(f"localhost:{port}")
    stub = helloworld_pb2_grpc.GreeterStub(channel)

    with patch("nucliadb_telemetry.grpc_sentry.capture_exception") as mock_capture_exception:
        response = await stub.SayHello(  # type: ignore
            helloworld_pb2.HelloRequest(name="you"),
        )
        assert response.message == "Hello, you!"
        assert mock_capture_exception.called is False


async def test_sentry_interceptor_without_streaming_errors(
    telemetry_grpc: GRPCTelemetry, grpc_service: int
):
    port = grpc_service
    channel = telemetry_grpc.init_client(f"localhost:{port}")
    stub = hellostreamingworld_pb2_grpc.MultiGreeterStub(channel)

    with patch("nucliadb_telemetry.grpc_sentry.capture_exception") as mock_capture_exception:
        async for response in stub.sayHello(  # type: ignore
            hellostreamingworld_pb2.HelloRequest(name="you")
        ):
            assert response.message == "Hello, you!"
            assert mock_capture_exception.called is False


async def test_sentry_interceptor_raises_unhandled_exception(
    telemetry_grpc: GRPCTelemetry, faulty_grpc_service: int
):
    port = faulty_grpc_service
    channel = telemetry_grpc.init_client(f"localhost:{port}")
    stub = helloworld_pb2_grpc.GreeterStub(channel)

    with patch("nucliadb_telemetry.grpc_sentry.capture_exception") as mock_capture_exception:
        with pytest.raises(AioRpcError):
            with pytest.raises(UnhanldedError):
                await stub.SayHello(  # type: ignore
                    helloworld_pb2.HelloRequest(name="you"),
                )
        assert mock_capture_exception.called is True
        assert mock_capture_exception.call_count == 1


async def test_sentry_interceptor_raises_unhandled_exception_stream(
    telemetry_grpc: GRPCTelemetry, faulty_grpc_service: int
):
    port = faulty_grpc_service
    channel = telemetry_grpc.init_client(f"localhost:{port}")
    stub = hellostreamingworld_pb2_grpc.MultiGreeterStub(channel)

    with patch("nucliadb_telemetry.grpc_sentry.capture_exception") as mock_capture_exception:
        with pytest.raises(AioRpcError):
            with pytest.raises(UnhanldedError):
                async for _ in stub.sayHello(  # type: ignore
                    hellostreamingworld_pb2.HelloRequest(name="you")
                ):
                    pass

        assert mock_capture_exception.called is True
        assert mock_capture_exception.call_count == 1
