from typing import Iterator
from nucliadb_protos.train_pb2 import TrainSentence
from nucliadb_dataset import CLIENT_ID
from nucliadb_dataset import NUCLIA_GLOBAL
from nucliadb_dataset.nuclia import NucliaDriver
from nucliadb_dataset.settings import settings


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
