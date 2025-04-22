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

import logging
from typing import Dict, Optional

import requests

from nucliadb_protos.dataset_pb2 import TrainSet

logger = logging.getLogger("nucliadb_dataset")

SIZE_BYTES = 4


class Streamer:
    resp: Optional[requests.Response]

    def __init__(
        self,
        trainset: TrainSet,
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
        self.resp = self.stream_session.post(
            f"{self.base_url}/v1/kb/{self.kbid}/trainset/{partition_id}",
            data=self.trainset.SerializeToString(),
            stream=True,
            timeout=None,
        )
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
