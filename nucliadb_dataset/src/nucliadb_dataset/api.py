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

from typing import Iterator, List

from nucliadb_dataset import CLIENT_ID, NUCLIA_GLOBAL
from nucliadb_dataset.nuclia import NucliaDriver
from nucliadb_dataset.settings import settings
from nucliadb_protos.train_pb2 import (
    TrainField,
    TrainInfo,
    TrainParagraph,
    TrainResource,
    TrainSentence,
)
from nucliadb_protos.writer_pb2 import GetEntitiesResponse, GetLabelsResponse


def get_nuclia_client() -> NucliaDriver:
    if CLIENT_ID not in NUCLIA_GLOBAL:
        NUCLIA_GLOBAL[CLIENT_ID] = NucliaDriver(settings.train_grpc_address)

    return NUCLIA_GLOBAL[CLIENT_ID]


def iterate_sentences(kbid: str, labels: bool, entities: bool, text: bool) -> Iterator[TrainSentence]:
    client = get_nuclia_client()
    for sentence in client.iterate_sentences(kbid, labels, entities, text):
        yield sentence


def iterate_paragraphs(kbid: str, labels: bool, entities: bool, text: bool) -> Iterator[TrainParagraph]:
    client = get_nuclia_client()
    for sentence in client.iterate_paragraphs(kbid, labels, entities, text):
        yield sentence


def iterate_fields(kbid: str, labels: bool, entities: bool, text: bool) -> Iterator[TrainField]:
    client = get_nuclia_client()
    for sentence in client.iterate_fields(kbid, labels, entities, text):
        yield sentence


def iterate_resources(kbid: str, labels: bool, entities: bool, text: bool) -> Iterator[TrainResource]:
    client = get_nuclia_client()
    for sentence in client.iterate_resources(kbid, labels, entities, text):
        yield sentence


def get_labels(kbid: str) -> GetLabelsResponse:
    client = get_nuclia_client()
    labels = client.get_labels(kbid)
    return labels


def get_entities(kbid: str) -> GetEntitiesResponse:
    client = get_nuclia_client()
    entities = client.get_entities(kbid)
    return entities


def get_info(kbid: str) -> TrainInfo:
    client = get_nuclia_client()
    info = client.get_info(kbid)
    return info


def get_ontology_count(kbid: str, paragraph_labelsets: List[str], resource_labelset: List[str]):
    client = get_nuclia_client()
    labels = client.get_ontology_count(kbid, paragraph_labelsets, resource_labelset)
    return labels
