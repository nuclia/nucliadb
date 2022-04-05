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
import aiofiles
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_utils.storages import CHUNK_SIZE


class LocalStorage:
    def __init__(self, local_testing_files: str):
        self.local_testing_files = local_testing_files

    async def download(self, cf: CloudFile):
        assert CloudFile.LOCAL == cf.source
        assert (
            self.local_testing_files is not None and self.local_testing_files in cf.uri
        )
        async with aiofiles.open(cf.uri, mode="rb") as f:
            while True:
                body = await f.read(CHUNK_SIZE)
                if body == b"" or body is None:
                    break
                else:
                    yield body
