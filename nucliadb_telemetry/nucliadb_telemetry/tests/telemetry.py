import os

import nats
import pytest
import requests
from fastapi import FastAPI
from httpx import AsyncClient
from nats.aio.msg import Msg
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_telemetry.grpc import OpenTelemetryGRPC
from nucliadb_telemetry.jetstream import JetStreamContextTelemetry
from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.tests.grpc import helloworld_pb2, helloworld_pb2_grpc
from nucliadb_telemetry.utils import (
    clean_telemetry,
    get_telemetry,
    init_telemetry,
    set_info_on_span,
)
from nucliadb_utils.fastapi.instrumentation import instrument_app

images.settings["jaeger"] = {
    "image": "jaegertracing/all-in-one",
    "version": "1.33",
    "options": {"ports": {"6831/udp": None, "16686": None}},
}


class Jaeger(BaseImage):
    name = "jaeger"
    port = 6831

    def get_port(self, port=None):
        if os.environ.get("TESTING", "") == "jenkins" or "TRAVIS" in os.environ:
            return port if port else self.port
        network = self.container_obj.attrs["NetworkSettings"]
        service_port = "{0}/udp".format(port if port else self.port)
        for netport in network["Ports"].keys():
            if netport == service_port:
                return network["Ports"][service_port][0]["HostPort"]

    def get_http_port(self, port=None):
        if os.environ.get("TESTING", "") == "jenkins" or "TRAVIS" in os.environ:
            return 16686
        network = self.container_obj.attrs["NetworkSettings"]
        service_port = "16686/tcp"
        for netport in network["Ports"].keys():
            if netport == service_port:
                return network["Ports"][service_port][0]["HostPort"]

    def check(self):
        resp = requests.get(f"http://{self.host}:{self.get_http_port()}")
        return resp.status_code == 200


@pytest.fixture(scope="function")
async def jaeger_server():
    server = Jaeger()
    server.run()
    yield server
    server.stop()


@pytest.fixture(scope="function")
async def settings(jaeger_server: Jaeger):
    telemetry_settings.jaeger_enabled = True
    telemetry_settings.jaeger_agent_host = "127.0.0.1"
    telemetry_settings.jaeger_agent_port = jaeger_server.get_port()
    telemetry_settings.jaeger_query_port = jaeger_server.get_http_port()


@pytest.fixture(scope="function")
async def telemetry_grpc(settings):
    tracer_provider = get_telemetry("GRPC_SERVICE")
    await init_telemetry(tracer_provider)
    util = OpenTelemetryGRPC("test_telemetry", tracer_provider)
    yield util


class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def __init__(self, natsd):
        self.natsd = natsd
        self.nc = None
        self.subscription = None
        self.tracer_provider = None
        self.messages = []

    async def subscription_worker(self, msg: Msg):
        tracer = self.tracer_provider.get_tracer("message_worker")
        with tracer.start_as_current_span("message_worker_span") as _:
            self.messages.append(msg)

    async def initialize(self):
        self.nc = await nats.connect(servers=[self.natsd])
        self.js = self.nc.jetstream()
        try:
            await self.js.delete_stream(name="testing")
        except nats.js.errors.NotFoundError:
            pass

        await self.js.add_stream(name="testing", subjects=["testing.*"])

        self.tracer_provider = get_telemetry("NATS_SERVICE")
        await init_telemetry(self.tracer_provider)
        self.jsotel = JetStreamContextTelemetry(
            self.js, "nats_service", self.tracer_provider
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
async def greeter(settings, natsd: str):
    obj = Greeter(natsd)
    await obj.initialize()
    yield obj
    await obj.finalize()


@pytest.fixture(scope="function")
async def grpc_service(telemetry_grpc: OpenTelemetryGRPC, greeter: Greeter):
    server = telemetry_grpc.init_server()
    helloworld_pb2_grpc.add_GreeterServicer_to_server(greeter, server)
    port = server.add_insecure_port("[::]:0")
    await server.start()
    yield port
    await server.stop(grace=True)


@pytest.fixture(scope="function")
async def http_service(settings, telemetry_grpc: OpenTelemetryGRPC, grpc_service: int):
    tracer_provider = get_telemetry("HTTP_SERVICE")
    await init_telemetry(tracer_provider)
    app = FastAPI(title="Test API")  # type: ignore
    set_global_textmap(B3MultiFormat())
    instrument_app(app, tracer_provider=tracer_provider, excluded_urls=[])

    @app.get("/")
    async def simple_api():
        set_info_on_span({"my.data": "is this"})
        tracer = tracer_provider.get_tracer(__name__)
        with tracer.start_as_current_span("simple_api_work") as _:
            channel = telemetry_grpc.init_client(f"localhost:{grpc_service}")
            stub = helloworld_pb2_grpc.GreeterStub(channel)
            response = await stub.SayHello(helloworld_pb2.HelloRequest(name="you"))
        return response.message

    client_base_url = "http://test"
    client = AsyncClient(app=app, base_url=client_base_url)  # type: ignore
    yield client
    await clean_telemetry("HTTP_SERVICE")
