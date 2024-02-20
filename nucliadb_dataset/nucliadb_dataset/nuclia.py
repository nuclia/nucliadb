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

from typing import Iterator, List

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
        for sentence in self.stub.GetSentences(request):
            yield sentence

    def iterate_paragraphs(
        self, kbid: str, labels: bool, entities: bool, text: bool
    ) -> Iterator[TrainParagraph]:
        request = GetParagraphsRequest()
        request.kb.uuid = kbid
        request.metadata.labels = labels
        request.metadata.entities = entities
        request.metadata.text = text
        for paragraph in self.stub.GetParagraphs(request):
            yield paragraph

    def iterate_resources(
        self, kbid: str, labels: bool, entities: bool, text: bool
    ) -> Iterator[TrainResource]:
        request = GetResourcesRequest()
        request.kb.uuid = kbid
        request.metadata.labels = labels
        request.metadata.entities = entities
        request.metadata.text = text
        for resource in self.stub.GetResources(request):
            yield resource

    def iterate_fields(
        self, kbid: str, labels: bool, entities: bool, text: bool
    ) -> Iterator[TrainField]:
        request = GetFieldsRequest()
        request.kb.uuid = kbid
        request.metadata.labels = labels
        request.metadata.entities = entities
        request.metadata.text = text
        for field in self.stub.GetFields(request):
            yield field

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
        self, kbid: str, paragraph_labelsets: List[str], resource_labelsets: List[str]
    ) -> LabelsetsCount:
        request = GetLabelsetsCountRequest()
        request.kb.uuid = kbid
        return self.stub.GetOntologyCount(request)
