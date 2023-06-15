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
#
from typing import Optional

import aiohttp
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_utils.storages import CHUNK_SIZE


class NucliaStorage:
    chunk_size = CHUNK_SIZE

    def __init__(
        self,
        nuclia_public_url: str,
        nuclia_zone: str,
        service_account: Optional[str] = None,
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
            assert resp.status == 200
            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                yield chunk

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def finalize(self):
        await self.session.close()
