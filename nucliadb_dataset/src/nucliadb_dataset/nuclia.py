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

from collections.abc import Iterator

import grpc

from nucliadb_protos.train_pb2 import (
    GetFieldsRequest,
    GetInfoRequest,
    GetLabelsetsCountRequest,
    GetParagraphsRequest,
    GetResourcesRequest,
    GetSentencesRequest,
    LabelsetsCount,
    TrainField,
    TrainInfo,
    TrainParagraph,
    TrainResource,
    TrainSentence,
)
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.writer_pb2 import (
    GetEntitiesRequest,
    GetEntitiesResponse,
    GetLabelsRequest,
    GetLabelsResponse,
)


class NucliaDriver:
    def __init__(self, grpc_address: str):
        self.grpc_address = grpc_address

        self.channel = grpc.insecure_channel(grpc_address)
        self.stub = TrainStub(self.channel)

    def iterate_sentences(
        self, kbid: str, labels: bool, entities: bool, text: bool
    ) -> Iterator[TrainSentence]:
        request = GetSentencesRequest()
        request.kb.uuid = kbid
        request.metadata.labels = labels
        request.metadata.entities = entities
        request.metadata.text = text
        yield from self.stub.GetSentences(request)

    def iterate_paragraphs(
        self, kbid: str, labels: bool, entities: bool, text: bool
    ) -> Iterator[TrainParagraph]:
        request = GetParagraphsRequest()
        request.kb.uuid = kbid
        request.metadata.labels = labels
        request.metadata.entities = entities
        request.metadata.text = text
        yield from self.stub.GetParagraphs(request)

    def iterate_resources(
        self, kbid: str, labels: bool, entities: bool, text: bool
    ) -> Iterator[TrainResource]:
        request = GetResourcesRequest()
        request.kb.uuid = kbid
        request.metadata.labels = labels
        request.metadata.entities = entities
        request.metadata.text = text
        yield from self.stub.GetResources(request)

    def iterate_fields(
        self, kbid: str, labels: bool, entities: bool, text: bool
    ) -> Iterator[TrainField]:
        request = GetFieldsRequest()
        request.kb.uuid = kbid
        request.metadata.labels = labels
        request.metadata.entities = entities
        request.metadata.text = text
        yield from self.stub.GetFields(request)

    def get_labels(self, kbid: str) -> GetLabelsResponse:
        request = GetLabelsRequest()
        request.kb.uuid = kbid
        return self.stub.GetOntology(request)

    def get_entities(self, kbid: str) -> GetEntitiesResponse:
        request = GetEntitiesRequest()
        request.kb.uuid = kbid
        return self.stub.GetEntities(request)

    def get_info(self, kbid: str) -> TrainInfo:
        request = GetInfoRequest()
        request.kb.uuid = kbid
        return self.stub.GetInfo(request)

    def get_ontology_count(
        self, kbid: str, paragraph_labelsets: list[str], resource_labelsets: list[str]
    ) -> LabelsetsCount:
        request = GetLabelsetsCountRequest()
        request.kb.uuid = kbid
        return self.stub.GetOntologyCount(request)
