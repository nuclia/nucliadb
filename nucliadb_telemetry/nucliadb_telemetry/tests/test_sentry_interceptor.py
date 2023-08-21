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

from unittest.mock import patch

import pytest
from grpc.aio import AioRpcError  # type: ignore

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
@pytest.mark.asyncio
async def grpc_service(telemetry_grpc: GRPCTelemetry):
    greeter = SimpleGreeter()
    server, port = await run_service(telemetry_grpc, greeter)
    yield port
    await server.stop(grace=True)


@pytest.fixture(scope="function")
@pytest.mark.asyncio
async def faulty_grpc_service(telemetry_grpc: GRPCTelemetry):
    greeter = FaultyGreeter()
    server, port = await run_service(telemetry_grpc, greeter)
    yield port
    await server.stop(grace=True)


@pytest.mark.asyncio
async def test_sentry_interceptor_without_errors(
    telemetry_grpc: GRPCTelemetry, grpc_service: int
):
    port = grpc_service
    channel = telemetry_grpc.init_client(f"localhost:{port}")
    stub = helloworld_pb2_grpc.GreeterStub(channel)

    with patch(
        "nucliadb_telemetry.grpc_sentry.capture_exception"
    ) as mock_capture_exception:
        response = await stub.SayHello(  # type: ignore
            helloworld_pb2.HelloRequest(name="you"),
        )
        assert response.message == "Hello, you!"
        assert mock_capture_exception.called is False


@pytest.mark.asyncio
async def test_sentry_interceptor_without_streaming_errors(
    telemetry_grpc: GRPCTelemetry, grpc_service: int
):
    port = grpc_service
    channel = telemetry_grpc.init_client(f"localhost:{port}")
    stub = hellostreamingworld_pb2_grpc.MultiGreeterStub(channel)

    with patch(
        "nucliadb_telemetry.grpc_sentry.capture_exception"
    ) as mock_capture_exception:
        async for response in stub.sayHello(  # type: ignore
            hellostreamingworld_pb2.HelloRequest(name="you")
        ):
            assert response.message == "Hello, you!"
            assert mock_capture_exception.called is False


@pytest.mark.asyncio
async def test_sentry_interceptor_raises_unhandled_exception(
    telemetry_grpc: GRPCTelemetry, faulty_grpc_service: int
):
    port = faulty_grpc_service
    channel = telemetry_grpc.init_client(f"localhost:{port}")
    stub = helloworld_pb2_grpc.GreeterStub(channel)

    with patch(
        "nucliadb_telemetry.grpc_sentry.capture_exception"
    ) as mock_capture_exception:
        with pytest.raises(AioRpcError):
            with pytest.raises(UnhanldedError):
                await stub.SayHello(  # type: ignore
                    helloworld_pb2.HelloRequest(name="you"),
                )
        assert mock_capture_exception.called is True
        assert mock_capture_exception.call_count == 1


@pytest.mark.asyncio
async def test_sentry_interceptor_raises_unhandled_exception_stream(
    telemetry_grpc: GRPCTelemetry, faulty_grpc_service: int
):
    port = faulty_grpc_service
    channel = telemetry_grpc.init_client(f"localhost:{port}")
    stub = hellostreamingworld_pb2_grpc.MultiGreeterStub(channel)

    with patch(
        "nucliadb_telemetry.grpc_sentry.capture_exception"
    ) as mock_capture_exception:
        with pytest.raises(AioRpcError):
            with pytest.raises(UnhanldedError):
                async for _ in stub.sayHello(  # type: ignore
                    hellostreamingworld_pb2.HelloRequest(name="you")
                ):
                    pass

        assert mock_capture_exception.called is True
        assert mock_capture_exception.call_count == 1
