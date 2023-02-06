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

from typing import Optional

import requests
from nucliadb_protos.dataset_pb2 import TrainSet
from urllib3.exceptions import ProtocolError

from nucliadb_sdk.client import NucliaDBClient

SIZE_BYTES = 4


class StreamerAlreadyRunning(Exception):
    pass


class Streamer:
    resp: Optional[requests.Response]
    client: NucliaDBClient

    def __init__(self, trainset: TrainSet, client: NucliaDBClient):
        self.client = client
        self.base_url = str(self.client.train_session.base_url).strip("/")
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
        if self.resp is None:
            raise Exception("Not initialized")
        try:
            header = self.resp.raw.read(4, decode_content=True)
            payload_size = int.from_bytes(header, byteorder="big", signed=False)
            data = self.resp.raw.read(payload_size)
        except ProtocolError:
            data = None
        return data

    def __next__(self) -> Optional[bytes]:
        payload = self.read()
        if payload in [None, b""]:
            raise StopIteration
        return payload
