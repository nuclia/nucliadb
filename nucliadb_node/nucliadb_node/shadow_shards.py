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
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Dict, Optional, Set, Tuple, Union

import aiofiles
from aiofiles import os as aos
from nucliadb_protos.noderesources_pb2 import Resource
from pydantic import BaseModel

from nucliadb_node import logger

SHADOW_SHARDS_FOLDER = "{data_path}/shadow_shards/"

MAIN: Dict[str, Any] = {}


class ShadowShardInfo(BaseModel):
    shard_id: str
    created_at: datetime = datetime.now()


class ShadowMetadata(BaseModel):
    file_id: str = "metadata.json"
    shards: Dict[str, ShadowShardInfo] = {}

    @classmethod
    async def load(cls, file_path: str) -> "ShadowMetadata":
        try:
            async with aiofiles.open(file_path, mode="r") as f:
                return cls.parse_raw(await f.read())
        except FileNotFoundError:
            return cls()

    async def save(self, file_path: str):
        async with aiofiles.open(file_path, mode="w") as f:
            await f.write(self.json())

    def get_info(self, shard_id: str) -> Optional[ShadowShardInfo]:
        return self.shards.get(shard_id)


class ShadowShardNotFound(Exception):
    pass


class ShadowShardsNotLoaded(Exception):
    pass


class OperationCode(str, Enum):
    SET = "SET"
    DELETE = "DEL"


NodeOperation = Tuple[OperationCode, Union[Resource, str]]


class ShadowShardsManager:
    """
    This class is responsible for managing the shadow shards of a particular node.
    A shadow shard is essentially a temporary file where we store all the operations (set
    or delete resources) for a shard that is being rebalanced or upgraded.
    """

    def __init__(self, folder: str):
        self._folder: str = folder
        self.shards: Set[str] = set()
        self._loaded: bool = False
        self._metadata_file: str = "metadata.json"
        self._metadata: ShadowMetadata = ShadowMetadata()

    @property
    def folder(self) -> str:
        if not self._folder.endswith("/"):
            self._folder += "/"
        return self._folder

    async def load(self) -> None:
        if self.loaded:
            return

        # Create shards folder if it doesn't exist
        await aos.makedirs(self.folder, exist_ok=True)

        await self.load_metadata()
        await self.load_shards()
        self._loaded = True

    async def load_shards(self):
        self.shards = set()
        for shard_path in glob.glob(f"{self.folder}*"):
            shard_id = shard_path.split(self._folder)[-1].lstrip("/")
            if shard_id != self._metadata_file:
                self.shards.add(shard_id)

    async def load_metadata(self) -> None:
        metadata_path = self.shard_path(self._metadata_file)
        self._metadata = await ShadowMetadata.load(metadata_path)

    async def save_metadata(self) -> None:
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        metadata_path = self.shard_path(self._metadata_file)
        await self.metadata.save(metadata_path)

    @property
    def loaded(self) -> bool:
        return self._loaded

    @property
    def metadata(self) -> ShadowMetadata:
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        return self._metadata

    def shard_path(self, shard_id: str) -> str:
        return f"{self.folder}{shard_id}"

    async def create(self) -> str:
        if not self.loaded:
            raise ShadowShardsNotLoaded()

        # Get a unique shard id
        shard_id = uuid.uuid4().hex
        while shard_id in self.shards:
            shard_id = uuid.uuid4().hex

        shard_path = self.shard_path(shard_id)
        await aos.mkdir(shard_path)

        self.shards.add(shard_id)
        self.metadata.shards[shard_id] = ShadowShardInfo(shard_id=shard_id)
        await self.save_metadata()
        return shard_id

    async def delete(self, shard_id: str) -> None:
        if not self.loaded:
            raise ShadowShardsNotLoaded()

        if shard_id not in self.shards:
            raise ShadowShardNotFound()

        shard_path = self.shard_path(shard_id)
        await aos.rmdir(shard_path)

        self.shards.remove(shard_id)
        self.metadata.shards.pop(shard_id)
        await self.save_metadata()

    def exists(self, shard_id: str) -> bool:
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        return shard_id in self.shards

    def get_op_id(self, txid: str, opcode: OperationCode, uuid: str) -> str:
        return f"{txid}_{opcode}_{uuid}"

    def parse_op_id(self, op_id: str) -> Tuple[str, OperationCode, str]:
        txid, opcode, uuid = op_id.split("_")
        return txid, OperationCode(opcode), uuid

    async def set_resource(self, brain: Resource, shard_id: str, txid: str) -> None:
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        if not self.exists(shard_id):
            raise ShadowShardNotFound()

        op_id = self.get_op_id(txid, OperationCode.SET, brain.resource.uuid)
        shard_path = self.shard_path(shard_id)
        async with aiofiles.open(f"{shard_path}/{op_id}", mode="wb") as f:
            await f.write(brain.SerializeToString())

    async def delete_resource(self, uuid: str, shard_id: str, txid: str) -> None:
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        if not self.exists(shard_id):
            raise ShadowShardNotFound()

        op_id = self.get_op_id(txid, OperationCode.DELETE, uuid)
        shard_path = self.shard_path(shard_id)
        async with aiofiles.open(f"{shard_path}/{op_id}", mode="x"):
            pass

    async def iter_operations(self, shard_id: str) -> AsyncIterator[NodeOperation]:
        """
        Iterates the operations stored in the shard folder ordered by transaction id (txid)
        """
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        if not self.exists(shard_id):
            raise ShadowShardNotFound()

        shard_path = self.shard_path(shard_id)

        def get_filename(op_path: str) -> str:
            return op_path.split("/")[-1]

        def get_txid(op_path: str) -> int:
            filename = get_filename(op_path)
            return int(filename.split("_")[0])

        op_paths = glob.glob(f"{shard_path}/*")
        op_paths_sorted_by_txid = sorted(op_paths, key=lambda p: get_txid(p))
        for path in op_paths_sorted_by_txid:
            op_id = get_filename(path)
            _, opcode, uuid = self.parse_op_id(op_id)
            if opcode == OperationCode.SET:
                async with aiofiles.open(path, mode="rb") as f:
                    payload = await f.read()
                    brain = Resource.FromString(payload)
                    yield opcode, brain
            elif opcode == OperationCode.DELETE:
                yield opcode, uuid


def get_data_path() -> str:
    data_path = os.environ.get("DATA_PATH")
    if not data_path:
        logger.warning(f"DATA_PATH not configured. Defaulting to: /data")
        data_path = "/data"
    data_path.rstrip("/")
    return data_path


def get_shadow_shards_manager() -> ShadowShardsManager:
    if "manager" not in MAIN:
        folder = SHADOW_SHARDS_FOLDER.format(data_path=get_data_path())
        MAIN["manager"] = ShadowShardsManager(folder=folder)
    manager = MAIN.get("manager")
    if manager is None:
        raise AttributeError()
    return manager
