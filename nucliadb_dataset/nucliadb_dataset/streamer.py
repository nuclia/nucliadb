from typing import Any, Callable, List, Tuple, Union
import requests
from nucliadb_protos.train_pb2 import (
    ParagraphClassificationBatch,
    TextLabel,
    TokensClassification,
    TrainResponse,
    TrainSet,
    Type,
)

from nucliadb_sdk.client import NucliaDBClient
from sklearn.preprocessing import MultiLabelBinarizer

SIZE_BYTES = 4


class Streamer:
    session: requests.Session
    resp: requests.Response
    client: NucliaDBClient

    def __init__(
        self, trainset: TrainSet, client: NucliaDBClient, generator: bool = False
    ):
        self.client = client
        self.trainset = trainset
        self.session = requests.Session()
        self.first = False
        self.train_size = None
        self.test_size = None
        self.total_train_batches = None
        self.total_test_batches = None
        self.count_train_batches = None
        self.count_test_batches = None
        self.test_phase = False
        self.generator = generator
        self.base_url = self.client.url
        self.mappings = []
        self.actual_pcb = None
        self.actual_pcb_index = 0

        if self.trainset.type == Type.PARAGRAPH_CLASSIFICATION:
            self.klass = ParagraphClassificationBatch
            self.map = self.paragraph_classifier_map
            self.labels = self.get_labels()
            self.mlb = MultiLabelBinarizer(classes=self.labels.values())

    def paragraph_classifier_map(self, pcb: ParagraphClassificationBatch):
        for entry in pcb.data:
            pass

    def get_labels(self):
        return self.client.reader_session.get(f"{self.base_url}/labelsets").json()

    def get_partitions(self):
        return self.client.reader_session.get(f"{self.base_url}/trainset").json()

    def initialize(self):
        self.resp = self.client.reader_session.stream(
            "GET",
            f"{self.base_url}/trainset",
            stream=True,
        )
        self.first = False
        self.test_phase = False

    def finalize(self):
        self.resp.close()

    def get_data(self):
        self.initialize()
        for data in self:
            yield data

        self.finalize()

    def __iter__(self):
        return self.next()

    def set_mappings(self, funcs: List[Callable[[Any, Any], Tuple[Any, Any]]]):
        self.mappings = funcs

    def apply_mapping(self, X, Y):
        for func in self.mappings:
            X, Y = func(X, Y)
        return X, Y

    def next(self) -> Tuple[Any, Any]:
        if self.actual_pcb is None or self.actual_pcb_index == len(
            self.actual_pcb.data
        ):
            self.actual_pcb = self.next_batch()
            if self.actual_pcb is None:
                return None
            self.actual_pcb_index = 0

        row: Union[TextLabel, TokensClassification] = self.actual_pcb.data.index(
            self.actual_pcb_index
        )
        X = row.text
        Y = row.labels
        self.actual_pcb_index += 1
        return self.apply_mapping(X, Y)

    def next_batch(self):
        data = self.resp.raw.stream(4, decode_content=True)
        if data is None:
            return

        payload_size = int.from_bytes(data, byteorder="big", signed=False)
        payload = self.resp.raw.stream(payload_size)
        if self.first is False:
            # Lets get the train/test size
            tr = TrainResponse()
            tr.ParseFromString(payload)
            self.train_size = tr.train
            self.test_size = tr.test
            self.total_train_batches = (tr.train // 2) + 1
            self.total_test_batches = (tr.test // 2) + 1
            self.count_train_batches = 0
            self.count_test_batches = 0
            self.first = True
            data = self.resp.raw.stream(4, decode_content=True)
            if data is None:
                return
            payload_size = int.from_bytes(data, byteorder="big", signed=False)
            payload = self.resp.raw.stream(payload_size)

        pcb = self.klass()
        pcb.ParseFromString(payload)

        if (
            self.count_train_batches < self.total_train_batches
            and self.test_phase is False
        ):
            self.count_train_batches += 1
        elif (
            self.count_train_batches == self.total_train_batches
            and self.test_phase is False
        ):
            # Switch to test
            self.test_phase = True
            return None
        elif self.count_train_batches == self.total_train_batches and self.test_phase:
            if self.count_test_batches < self.total_test_batches:
                self.count_test_batches += 1
            else:
                return None
        return pcb
