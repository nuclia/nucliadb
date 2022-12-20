import requests
from nucliadb_protos.train_pb2 import (
    ParagraphClassificationBatch,
    TrainResponse,
    TrainSet,
    Type,
)

from nucliadb_sdk.client import NucliaDBClient

SIZE_BYTES = 4


class Streamer:
    session: requests.Session
    resp: requests.Response
    client: NucliaDBClient

    def __init__(self, trainset: TrainSet, client: NucliaDBClient):
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

        if self.trainset.type == Type.PARAGRAPH_CLASSIFICATION:
            self.klass = ParagraphClassificationBatch

    def get_partitions(self):
        return self.client.reader_session.get(
            f"{self.base_url}/{self.trainset.kbid}/trainset"
        ).json()

    def initialize(self):
        self.resp = self.client.reader_session.stream(
            "GET",
            f"{self.base_url}/{self.trainset.kbid}/trainset",
            headers=self.headers,
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

    def next(self):
        data = self.resp.raw.stream(4, decode_content=True)
        if data is None:
            return

        payload_size = int.from_bytes(data, byteorder="big", signed=False)
        payload = self.resp.raw.stream(payload_size)
        if self.first is False:
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
