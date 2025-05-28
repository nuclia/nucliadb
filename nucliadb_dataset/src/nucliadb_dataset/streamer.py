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

import logging
from typing import Dict, Optional, Union

import requests

from nucliadb_models.trainset import TrainSet as TrainSetModel
from nucliadb_protos.dataset_pb2 import TrainSet as TrainSetPB

logger = logging.getLogger("nucliadb_dataset")

SIZE_BYTES = 4


class Streamer:
    resp: Optional[requests.Response]

    def __init__(
        self,
        trainset: Union[TrainSetPB, TrainSetModel],
        reader_headers: Dict[str, str],
        base_url: str,
        kbid: str,
    ):
        self.reader_headers = reader_headers
        self.base_url = base_url
        self.trainset = trainset
        self.kbid = kbid
        self.resp = None

    @property
    def initialized(self):
        return self.resp is not None

    def initialize(self, partition_id: str):
        self.stream_session = requests.Session()
        self.stream_session.headers.update(self.reader_headers)
        if isinstance(self.trainset, TrainSetPB):
            # Legacy version of the endpoint is passing the protobuffer as bytes in the request content
            self.resp = self.stream_session.post(
                f"{self.base_url}/v1/kb/{self.kbid}/trainset/{partition_id}",
                data=self.trainset.SerializeToString(),
                stream=True,
                timeout=None,
            )
        elif isinstance(self.trainset, TrainSetModel):
            self.resp = self.stream_session.post(
                f"{self.base_url}/v1/kb/{self.kbid}/trainset/{partition_id}",
                json=self.trainset.model_dump(),
                stream=True,
                timeout=None,
            )
        else:  # pragma: no cover
            raise ValueError("Invalid trainset type")
        self.resp.raise_for_status()

    def finalize(self):
        if self.resp is not None:
            self.resp.close()
        self.resp = None

    def __iter__(self):
        return self

    def read(self) -> Optional[bytes]:
        assert self.resp is not None, "Streamer not initialized"
        header = self.resp.raw.read(4, decode_content=True)
        if header == b"":
            return None
        payload_size = int.from_bytes(header, byteorder="big", signed=False)  # noqa
        data = self.resp.raw.read(payload_size)
        return data

    def __next__(self) -> Optional[bytes]:
        payload = self.read()
        if payload in [None, b""]:
            logger.info("Streamer finished reading")
            raise StopIteration
        return payload
