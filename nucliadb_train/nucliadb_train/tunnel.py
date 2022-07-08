import asyncio
from typing import Optional

from grpc import ChannelCredentials, aio, ssl_channel_credentials
from nucliadb_protos.train_pb2 import CloseRequest, MessageFromServer, MessageToServer

from nucliadb_protos import train_pb2_grpc
from nucliadb_telemetry.grpc import OpenTelemetryGRPC
from nucliadb_telemetry.utils import get_telemetry, init_telemetry
from nucliadb_train.servicer import TrainServicer


class TunnelServicer:
    def __init__(
        self,
        service_name: str,
        grpc_address: str,
        root_certificates: Optional[bytes] = None,
        private_key: Optional[bytes] = None,
        certificate_chain: Optional[bytes] = None,
    ):
        self.service_name = service_name
        self.grpc_address = grpc_address
        self.credentials: ChannelCredentials = ssl_channel_credentials(
            root_certificates, private_key, certificate_chain
        )
        self.channel = None
        self.call = None
        self.task = None
        self.servicer = None
        self.stub: Optional[train_pb2_grpc.TunnelStub] = None

    async def connect(self, key):
        if self.task is not None:
            raise AttributeError("Already connected")

        if self.servicer is not None:
            raise AttributeError("Already initialized")

        tracer_provider = get_telemetry(self.service_name)

        if tracer_provider is not None:
            await init_telemetry(tracer_provider)
            otgrpc = OpenTelemetryGRPC(f"{self.service_name}_train", tracer_provider)
            self.channel = otgrpc.init_client(self.grpc_address)
        else:
            self.channel = aio.secure_channel(self.grpc_address, self.credentials)

        self.servicer = TrainServicer()
        await self.servicer.initialize()
        login = MessageToServer()
        login.register_request.key = key
        self.stub = train_pb2_grpc.TunnelStub(self.channel)

        self.call = self.stub.Tunnel()
        await self.call.write(login)
        response: MessageFromServer = await self.call.read()
        assert response.register_response.status
        self.task = asyncio.create_task(self.loop())

    async def loop(self):
        while True:
            response: MessageFromServer = await self.call.read()
            if response.HasField("close_request"):
                break
            elif response.HasField("get_sentences_request"):
                async for sentence in self.servicer.GetSentences(
                    response.get_sentences_request
                ):
                    message = MessageToServer()
                    message.sentence.CopyFrom(sentence)
                    await self.call.write(message)
                message = MessageToServer()
                message.end_of_stream = True
                await self.call.write(message)
            elif response.HasField("get_paragraphs_request"):
                async for paragraph in self.servicer.GetParagraphs(
                    response.get_paragraphs_request
                ):
                    message = MessageToServer()
                    message.paragraph.CopyFrom(paragraph)
                    await self.call.write(message)
                message = MessageToServer()
                message.end_of_stream = True
                await self.call.write(message)
            elif response.HasField("get_fields_request"):
                async for field in self.servicer.GetFields(response.get_fields_request):
                    message = MessageToServer()
                    message.field.CopyFrom(field)
                    await self.call.write(message)
                message = MessageToServer()
                message.end_of_stream = True
                await self.call.write(message)
            elif response.HasField("get_resources_request"):
                async for resource in self.servicer.GetResources(
                    response.get_resources_request
                ):
                    message = MessageToServer()
                    message.resource.CopyFrom(resource)
                    await self.call.write(message)
                message = MessageToServer()
                message.end_of_stream = True
                await self.call.write(message)
            elif response.HasField("get_labels_request"):
                labels = self.servicer.GetOntology(response.get_labels_request)
                message = MessageToServer()
                message.labels.CopyFrom(labels)
                await self.call.write(message)
            elif response.HasField("get_entities_request"):
                entities = self.servicer.GetEntities(response.get_entities_request)
                message = MessageToServer()
                message.entities.CopyFrom(entities)
                await self.call.write(message)

        await self.call.done_writing()
        self.servicer = None
        self.task = None

    async def disconnect(self):
        if self.task is not None:
            self.task.cancel()
            self.task = None
        if self.call is not None:
            goodbye = MessageToServer()
            goodbye.close_request = CloseRequest()
            await self.call.write(goodbye)
            await self.call.done_writing()
