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

import asyncio
import os
from typing import AsyncIterator

import nats
import pytest
import requests
from fastapi import FastAPI
from grpc import aio
from httpx import AsyncClient
from nats.aio.msg import Msg
from nats.js import api
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_telemetry.fastapi import instrument_app
from nucliadb_telemetry.grpc import GRPCTelemetry
from nucliadb_telemetry.jetstream import JetStreamContextTelemetry, NatsClientTelemetry
from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.tests.grpc import (
    hellostreamingworld_pb2,
    hellostreamingworld_pb2_grpc,
    helloworld_pb2,
    helloworld_pb2_grpc,
)
from nucliadb_telemetry.utils import (
    clean_telemetry,
    get_telemetry,
    init_telemetry,
    set_info_on_span,
)

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
async def set_telemetry_settings(jaeger_server: Jaeger):
    telemetry_settings.jaeger_enabled = True
    telemetry_settings.jaeger_agent_host = "127.0.0.1"
    telemetry_settings.jaeger_agent_port = jaeger_server.get_port()
    telemetry_settings.jaeger_query_port = jaeger_server.get_http_port()


@pytest.fixture(scope="function")
async def telemetry_grpc(set_telemetry_settings):
    tracer_provider = get_telemetry("GRPC_SERVICE")
    await init_telemetry(tracer_provider)
    util = GRPCTelemetry("test_telemetry", tracer_provider)
    yield util
    await clean_telemetry("GRPC_SERVICE")


class GreeterStreaming(hellostreamingworld_pb2_grpc.MultiGreeterServicer):
    def __init__(self, natsd):
        self.natsd = natsd
        self.nc = None
        self.push_subscription = None
        self.tracer_provider = None
        self.messages = []

    async def push_subscription_worker(self, msg: Msg):
        tracer = self.tracer_provider.get_tracer("message_worker")
        with tracer.start_as_current_span("message_worker_span") as _:
            self.messages.append(msg)

    async def initialize(self):
        self.nc = await nats.connect(servers=[self.natsd])
        self.js = self.nc.jetstream()

        try:
            await self.js.stream_info("testing")
        except nats.js.errors.NotFoundError:
            await self.js.add_stream(name="testing", subjects=["testing.*"])

        self.tracer_provider = get_telemetry("NATS_SERVICE")
        await init_telemetry(self.tracer_provider)
        self.jsotel = JetStreamContextTelemetry(
            self.js, "nats_service", self.tracer_provider
        )

        self.push_subscription = await self.jsotel.subscribe(
            subject="testing.stelemetry",
            stream="testing",
            cb=self.push_subscription_worker,
        )

    async def finalize(self):
        await self.push_subscription.unsubscribe()
        await self.nc.drain()
        await self.nc.close()

    async def sayHello(self, request, context):
        await self.jsotel.publish("testing.stelemetry", request.name.encode())
        for _ in range(10):
            yield hellostreamingworld_pb2.HelloReply(
                message="Hello, %s!" % request.name
            )


class Greeter(helloworld_pb2_grpc.GreeterServicer):
    def __init__(self, natsd):
        self.natsd = natsd
        self.nc = None
        self.push_subscription = None
        self.pull_subscription_one = None
        self.puller_task = None
        self.pubsub_subscription = None
        self.tracer_provider = None
        self.messages = []
        self.puller_task_one = None

    async def push_subscription_worker(self, msg: Msg):
        tracer = self.tracer_provider.get_tracer("message_worker")
        with tracer.start_as_current_span("message_worker_span") as _:
            self.messages.append(msg)

    async def pull_subscription_worker_one(self):
        async def callback(message):
            self.messages.append(message)

        await self.jsotel.pull_one(self.pull_subscription_one, callback)

    async def pubsub_subscription_worker(self, msg: Msg):
        tracer = self.tracer_provider.get_tracer("pubsub_worker")
        with tracer.start_as_current_span("pubsub_worker_span") as _:
            self.messages.append(msg)

    async def reqresp_subscription_worker(self, msg: Msg):
        tracer = self.tracer_provider.get_tracer("reqresp_worker")
        with tracer.start_as_current_span("reqresp_worker_span") as _:
            self.messages.append(msg)
            await msg.respond(b"Bye Bye!")

    async def initialize(self):
        self.nc = await nats.connect(servers=[self.natsd])
        self.js = self.nc.jetstream()

        try:
            await self.js.stream_info("testing")
        except nats.js.errors.NotFoundError:
            await self.js.add_stream(name="testing", subjects=["testing.*"])

        self.tracer_provider = get_telemetry("NATS_SERVICE")
        await init_telemetry(self.tracer_provider)
        self.jsotel = JetStreamContextTelemetry(
            self.js, "nats_service", self.tracer_provider
        )

        self.push_subscription = await self.jsotel.subscribe(
            subject="testing.telemetry",
            stream="testing",
            cb=self.push_subscription_worker,
        )

        # Nats Jetstream Pull subscription including consumer creation
        # and task to pull one message
        config = api.ConsumerConfig()
        config.filter_subject = "testing.telemetry_pull_one"
        config.durable_name = "testing_consumer_one"
        await self.js._jsm.add_consumer(stream="testing", config=config)

        self.pull_subscription_one = await self.jsotel.pull_subscribe(
            subject=config.filter_subject, durable=config.durable_name, stream="testing"
        )

        self.puller_task_one = asyncio.create_task(self.pull_subscription_worker_one())

        # Plain nats instrumentation and subscription
        # (no streams neither consumers used here)
        self.ncotel = NatsClientTelemetry(self.nc, "nats_service", self.tracer_provider)

        self.pubsub_subscription = await self.ncotel.subscribe(
            subject="testing.telemetry_nats_pubsub",
            queue="telemetry_nats_pubsub",
            cb=self.pubsub_subscription_worker,
        )

        # Plain nats request-response

        self.reqresp_subscription = await self.ncotel.subscribe(
            subject="testing.telemetry_nats_reqresp",
            queue="telemetry_nats_reqresp",
            cb=self.reqresp_subscription_worker,
        )

    async def finalize(self):
        await self.push_subscription.unsubscribe()

        await self.pull_subscription_one.unsubscribe()
        self.puller_task_one.cancel()
        await self.js._jsm.delete_consumer(
            stream="testing", consumer="testing_consumer_one"
        )

        await self.pubsub_subscription.unsubscribe()
        await self.nc.drain()
        await self.nc.close()

    async def SayHello(self, request, context):
        # Send message to test Jetstream publish and subscribe message
        await self.jsotel.publish("testing.telemetry", request.name.encode())

        # Send message to test Jetstream pull subscriber one
        await self.jsotel.publish("testing.telemetry_pull_one", request.name.encode())

        # Test regular nats pubsub
        await self.ncotel.publish(
            "testing.telemetry_nats_pubsub", request.name.encode()
        )

        # Test regular nats request-response
        await self.ncotel.request(
            "testing.telemetry_nats_reqresp", request.name.encode()
        )

        return helloworld_pb2.HelloReply(
            message=("Hello, %s!" % request.name) * 2_000_000
        )


@pytest.fixture(scope="function")
async def greeter(set_telemetry_settings, natsd: str) -> AsyncIterator[Greeter]:
    obj = Greeter(natsd)
    await obj.initialize()
    yield obj
    await obj.finalize()


@pytest.fixture(scope="function")
async def greeter_streaming(
    set_telemetry_settings, natsd: str
) -> AsyncIterator[GreeterStreaming]:
    obj = GreeterStreaming(natsd)
    await obj.initialize()
    yield obj
    await obj.finalize()


@pytest.fixture(scope="function")
async def grpc_service(
    telemetry_grpc: GRPCTelemetry,
    greeter: Greeter,
    greeter_streaming: GreeterStreaming,
):
    server = telemetry_grpc.init_server()
    helloworld_pb2_grpc.add_GreeterServicer_to_server(greeter, server)
    hellostreamingworld_pb2_grpc.add_MultiGreeterServicer_to_server(
        greeter_streaming, server
    )
    port = server.add_insecure_port("[::]:0")
    await server.start()
    yield port
    await server.stop(grace=True)


@pytest.fixture(scope="function")
async def http_service(
    set_telemetry_settings, telemetry_grpc: GRPCTelemetry, grpc_service: int
):
    tracer_provider = get_telemetry("HTTP_SERVICE")
    await init_telemetry(tracer_provider)
    app = FastAPI(title="Test API")  # type: ignore
    set_global_textmap(B3MultiFormat())
    instrument_app(
        app,
        tracer_provider=tracer_provider,
        excluded_urls=[],
        metrics=True,
        trace_id_on_responses=True,
    )

    @app.get("/")
    async def simple_api():
        set_info_on_span({"my.data": "is this"})
        tracer = tracer_provider.get_tracer(__name__)
        with tracer.start_as_current_span("simple_api_work") as _:
            channel = telemetry_grpc.init_client(f"localhost:{grpc_service}")
            stub = helloworld_pb2_grpc.GreeterStub(channel)
            response = await stub.SayHello(
                helloworld_pb2.HelloRequest(name="you"),
                # This metadata is here to make sure our instrumentor handles correctly
                # requests with metadata, as it does some manipulation of in on start_client_span
                # The servicer endpoint does not use this metadata
                metadata=aio.Metadata.from_tuple((("header1", "value1"),)),
            )
        with tracer.start_as_current_span("simple_stream_api_work") as _:
            channel = telemetry_grpc.init_client(f"localhost:{grpc_service}")
            stub = hellostreamingworld_pb2_grpc.MultiGreeterStub(channel)
            async for sresponse in stub.sayHello(
                helloworld_pb2.HelloRequest(name="you")
            ):
                assert sresponse
        return response.message

    client_base_url = "http://test"
    client = AsyncClient(app=app, base_url=client_base_url)  # type: ignore
    yield client
    await clean_telemetry("HTTP_SERVICE")
