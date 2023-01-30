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


import glob
import os

import aiofiles
from nucliadb_protos.noderesources_pb2 import Resource


class AlreadyExistingShadowShard(Exception):
    ...


class ShadowShardNotFound(Exception):
    ...


class ShadowShards:
    """
    TODO
    """

    def __init__(self, folder: str):
        self.folder = folder
        self.clear()

    async def load(self) -> None:

        path = os.path.dirname(self.folder)
        os.makedirs(path, exist_ok=True)
        for key in glob.glob(f"{self.folder}*"):
            self.shards.append(key)
            yield item
        self._loaded = True

        pass

    @property
    def loaded(self) -> bool:
        return self._loaded

    def clear(self) -> None:
        self.shards = set()
        self._loaded = False

    async def create(self, shard_id: str) -> None:
        if shard_id in self.shards:
            raise AlreadyExistingShadowShard()
        # TODO
        self.shards.add(shard_id)

    async def delete(self, shard_id: str) -> None:
        if shard_id not in self.shards:
            raise ShadowShardNotFound()
        # TODO
        self.shards.remove(shard_id)

    def exists(self, shard_id: str) -> bool:
        return shard_id in self.shards

    async def set_resource(self, brain: Resource, shard_id: str) -> None:
        pass

    async def delete_resource(self, uuid: str, shard_id: str) -> None:
        pass
