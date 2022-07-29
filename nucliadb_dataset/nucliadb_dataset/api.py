from typing import Iterator, List
from nucliadb_protos.train_pb2 import (
    TrainField,
    TrainInfo,
    TrainParagraph,
    TrainResource,
    TrainSentence,
)
from nucliadb_dataset import CLIENT_ID
from nucliadb_dataset import NUCLIA_GLOBAL
from nucliadb_dataset.nuclia import NucliaDriver
from nucliadb_dataset.settings import settings
from nucliadb_protos.writer_pb2 import GetLabelsResponse


def get_nuclia_client() -> NucliaDriver:
    if CLIENT_ID not in NUCLIA_GLOBAL:
        NUCLIA_GLOBAL[CLIENT_ID] = NucliaDriver(settings.train_grpc_address)

    return NUCLIA_GLOBAL[CLIENT_ID]


def iterate_sentences(
    kbid: str, labels: bool, entities: bool, text: bool
) -> Iterator[TrainSentence]:
    client = get_nuclia_client()
    for sentence in client.iterate_sentences(kbid, labels, entities, text):
        yield sentence


def iterate_paragraphs(
    kbid: str, labels: bool, entities: bool, text: bool
) -> Iterator[TrainParagraph]:
    client = get_nuclia_client()
    for sentence in client.iterate_paragraphs(kbid, labels, entities, text):
        yield sentence


def iterate_fields(
    kbid: str, labels: bool, entities: bool, text: bool
) -> Iterator[TrainField]:
    client = get_nuclia_client()
    for sentence in client.iterate_fields(kbid, labels, entities, text):
        yield sentence


def iterate_resources(
    kbid: str, labels: bool, entities: bool, text: bool
) -> Iterator[TrainResource]:
    client = get_nuclia_client()
    for sentence in client.iterate_resources(kbid, labels, entities, text):
        yield sentence


def get_labels(kbid: str) -> GetLabelsResponse:
    client = get_nuclia_client()
    labels = client.get_labels(kbid)
    return labels


def get_info(kbid: str) -> TrainInfo:
    client = get_nuclia_client()
    info = client.get_info(kbid)
    return info


def get_ontology_count(
    kbid: str, paragraph_labelsets: List[str], resource_labelset: List[str]
):
    client = get_nuclia_client()
    labels = client.get_ontology_count(kbid, paragraph_labelsets, resource_labelset)
    return labels
