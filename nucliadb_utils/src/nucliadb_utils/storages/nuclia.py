# Copyright 2021 Bosutech XXI S.L.
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

import aiohttp

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_utils.storages import CHUNK_SIZE


class NucliaStorage:
    chunk_size = CHUNK_SIZE

    def __init__(
        self,
        nuclia_public_url: str,
        nuclia_zone: str,
        service_account: str | None = None,
    ):
        self.service_account = service_account
        self.nuclia_public_url = nuclia_public_url.format(zone=nuclia_zone)
        self.nuclia_zone = nuclia_zone
        self._session = None

    async def download(self, cf: CloudFile):
        assert CloudFile.FLAPS == cf.source
        if self.service_account is None:
            raise AttributeError("Invalid service account")
        url = f"{self.nuclia_public_url}/api/v1/processing/download?token={cf.uri}"
        async with self.session.get(
            url,
            headers={
                "X-STF-NUAKEY": f"Bearer {self.service_account}",
            },
        ) as resp:
            resp.raise_for_status()
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                yield chunk

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def finalize(self):
        await self.session.close()
