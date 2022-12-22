from typing import Any, Optional, Tuple
from nucliadb_sdk.client import NucliaDBClient
import requests
from nucliadb_protos.train_pb2 import (
    TrainSet,
)

from urllib3.exceptions import ProtocolError


SIZE_BYTES = 4


class StreamerAlreadyRunning(Exception):
    pass


class Streamer:
    resp: requests.Response
    client: NucliaDBClient

    def __init__(self, trainset: TrainSet, client: NucliaDBClient):
        self.client = client
        self.base_url = self.client.url
        self.trainset = trainset
        self.resp = None

    @property
    def initialized(self):
        return self.resp is not None

    def initialize(self, partition_id: str):
        self.resp = self.client.stream_session.post(
            f"{self.base_url}/trainset/{partition_id}",
            data=self.trainset.SerializeToString(),
            stream=True,
        )

    def finalize(self):
        self.resp.close()
        self.resp = None

    def __iter__(self):
        return self

    def read(self) -> Optional[bytes]:
        try:
            header = self.resp.raw.read(4, decode_content=True)
            payload_size = int.from_bytes(header, byteorder="big", signed=False)
            data = self.resp.raw.read(payload_size)
        except ProtocolError:
            data = None
        return data

    def __next__(self) -> Tuple[Any, Any]:
        payload = self.read()
        if payload is None:
            raise StopIteration
        return payload
