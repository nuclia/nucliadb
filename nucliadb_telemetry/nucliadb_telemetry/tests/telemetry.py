import asyncio
import contextlib
import socket
from asyncio import DatagramProtocol

import nats
import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from nats.aio.msg import Msg
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat

from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.telemetry import (
    JetStreamContextTelemetry,
    OpenTelemetryGRPC,
    init_telemetry,
)
from nucliadb_telemetry.tests.grpc import helloworld_pb2, helloworld_pb2_grpc

JAEGGER_MESSAGES = []


class DefaultProtocol(DatagramProtocol):
    def datagram_received(self, data, _addr):
        JAEGGER_MESSAGES.append(data)


class UDPServer:
    __slots__ = ("_port", "_loop", "_protocol", "_transport")

    def __init__(
        self,
    ) -> None:

        self._port = _unused_udp_port()
        self._loop = asyncio.get_event_loop()
        self._transport = None

    @property
    def port(self):
        return self._port

    async def initialize(self):
        listen = self._loop.create_datagram_endpoint(
            DefaultProtocol, local_addr=("0.0.0.0", self._port)
        )
        self._transport, _ = await listen

    async def finalize(self):
        self._transport.close()
        self._transport = None


def _unused_udp_port() -> int:
    with contextlib.closing(socket.socket(type=socket.SOCK_DGRAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="function")
async def settings():
    server = UDPServer()
    await server.initialize()
    telemetry_settings.jeager_enabled = True
    telemetry_settings.jaeger_host = "127.0.0.1"
    telemetry_settings.jaeger_port = server.port
    yield
    await server.finalize()


@pytest.fixture(scope="function")
async def telemetry_grpc(settings):
    tracer_provider = init_telemetry("HTTP_SERVICE")
    util = OpenTelemetryGRPC("test_telemetry", tracer_provider)
    yield util


class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def __init__(self, natsd):
        self.natsd = natsd
        self.nc = None
        self.subscription = None
        self.messages = []

    async def subscription_worker(self, msg: Msg):
        self.messages.append(msg)

    async def initialize(self):
        self.nc = await nats.connect(servers=[self.natsd])
        self.js = self.nc.jetstream()
        try:
            await self.js.delete_stream(name="testing")
        except nats.js.errors.NotFoundError:
            pass

        await self.js.add_stream(name="testing", subjects=["testing.*"])

        tracer_provider = init_telemetry("NATS_SERVICE")
        self.jsotel = JetStreamContextTelemetry(
            self.js, "nats_service", tracer_provider
        )

        self.subscription = await self.jsotel.subscribe(
            subject="testing.telemetry",
            stream="testing",
            cb=self.subscription_worker,
        )

    async def finalize(self):
        await self.subscription.unsubscribe()
        await self.nc.drain()
        await self.nc.close()

    async def SayHello(self, request, context):
        await self.jsotel.publish("testing.telemetry", request.name.encode())
        return helloworld_pb2.HelloReply(message="Hello, %s!" % request.name)


@pytest.fixture(scope="function")
async def greeter(natsd: str):
    obj = Greeter(natsd)
    await obj.initialize()
    yield obj
    await obj.finalize()


@pytest.fixture(scope="function")
async def grpc_service(telemetry_grpc: OpenTelemetryGRPC, greeter: Greeter):
    server = telemetry_grpc.init_server()
    await greeter.initialize()
    helloworld_pb2_grpc.add_GreeterServicer_to_server(greeter, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    yield port
    await server.stop(grace=True)


@pytest.fixture(scope="function")
async def http_service(telemetry_grpc: OpenTelemetryGRPC, grpc_service: int):
    tracer_provider = init_telemetry("HTTP_SERVICE")
    app = FastAPI(title="Test API")  # type: ignore
    set_global_textmap(B3MultiFormat())
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)

    @app.get("/")
    async def simple_api():
        channel = telemetry_grpc.init_client(f"localhost:{grpc_service}")
        stub = helloworld_pb2_grpc.GreeterStub(channel)
        response = await stub.SayHello(helloworld_pb2.HelloRequest(name="you"))
        return response.message

    client_base_url = "http://test"
    client = AsyncClient(app=app, base_url=client_base_url)  # type: ignore
    yield client
