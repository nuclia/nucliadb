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


import base64
import glob
import os
import uuid
from enum import Enum
from typing import AsyncIterator, Set, Tuple, Union

import aiofiles
from aiofiles import os as aos
from nucliadb_protos.noderesources_pb2 import Resource


class ShadowShardNotFound(Exception):
    pass


class ShadowShardsNotLoaded(Exception):
    pass


class OperationCode(str, Enum):
    SET = "SET:"
    DELETE = "DEL:"

    @classmethod
    def from_str(cls, opcode_str):
        if opcode_str == cls.SET.value:
            return cls.SET
        elif opcode_str == cls.DELETE.value:
            return cls.DELETE
        raise ValueError(f"Unknown opcode: {opcode_str}")


NodeOperation = Tuple[OperationCode, Union[Resource, str]]

# singleton
SHADOW_SHARDS = None


class ShadowShards:
    """
    This class is responsible for handling the disk operations for shadow shards.
    Shadow shards is where we temporarily store the operations (set or delete resources)
    for a shard that is being rebalanced or upgraded.
    """

    def __init__(self, folder: str):
        self.folder: str = folder
        self.shards: Set[str] = set()
        self._loaded: bool = False

    async def load(self) -> None:
        if self.loaded:
            return
        path = os.path.dirname(self.folder)
        await aos.makedirs(path, exist_ok=True)
        for shard_path in glob.glob(f"{self.folder}/*"):
            shard_id = shard_path.split(self.folder)[-1].lstrip("/")
            self.shards.add(shard_id)
        self._loaded = True

    @property
    def loaded(self) -> bool:
        return self._loaded

    def shard_path(self, shard_id: str) -> str:
        return f"{self.folder}/{shard_id}"

    async def create(self) -> str:
        if not self.loaded:
            raise ShadowShardsNotLoaded()

        # Get a unique shard id
        shard_id = uuid.uuid4().hex
        while shard_id in self.shards:
            shard_id = uuid.uuid4().hex

        async with aiofiles.open(self.shard_path(shard_id), mode="x"):
            pass

        self.shards.add(shard_id)
        return shard_id

    async def delete(self, shard_id: str) -> None:
        if not self.loaded:
            raise ShadowShardsNotLoaded()

        if shard_id not in self.shards:
            raise ShadowShardNotFound()

        shard_path = self.shard_path(shard_id)
        await aiofiles.os.remove(shard_path)
        self.shards.remove(shard_id)

    def exists(self, shard_id: str) -> bool:
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        return shard_id in self.shards

    def encode_operation(self, node_operation: NodeOperation) -> bytes:
        opcode, payload = node_operation
        if opcode == OperationCode.SET:
            payload = payload.SerializeToString()  # type: ignore
        elif opcode == OperationCode.DELETE:
            payload = payload.encode()  # type: ignore
        else:
            ValueError(f"Unknown opcode: {opcode}")
        encoded = base64.b64encode(payload).decode()  # type: ignore
        return (opcode + encoded + "\n").encode()

    def decode_operation(self, encoded: str) -> NodeOperation:
        opcode = OperationCode.from_str(encoded[:4])
        decoded_payload = base64.b64decode(encoded[4:])
        if opcode == OperationCode.SET:
            return (opcode, Resource.FromString(decoded_payload))
        elif opcode == OperationCode.DELETE:
            return (opcode, decoded_payload.decode())
        raise ValueError(f"Unknown opcode: {opcode}")

    async def set_resource(self, brain: Resource, shard_id: str) -> None:
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        if not self.exists(shard_id):
            raise ShadowShardNotFound()

        encoded = self.encode_operation((OperationCode.SET, brain))
        shard_path = self.shard_path(shard_id)
        async with aiofiles.open(shard_path, mode="ab") as f:
            await f.write(encoded)

    async def delete_resource(self, uuid: str, shard_id: str) -> None:
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        if not self.exists(shard_id):
            raise ShadowShardNotFound()

        encoded = self.encode_operation((OperationCode.DELETE, uuid))
        shard_path = self.shard_path(shard_id)
        async with aiofiles.open(shard_path, mode="ab") as f:
            await f.write(encoded)

    async def iter_operations(self, shard_id: str) -> AsyncIterator[NodeOperation]:
        if not self.loaded:
            raise ShadowShardsNotLoaded()
        if not self.exists(shard_id):
            raise ShadowShardNotFound()

        shard_path = self.shard_path(shard_id)
        async with aiofiles.open(shard_path, "r") as f:
            line = await f.readline()
            while line:
                yield self.decode_operation(line)
                line = await f.readline()


def get_shadow_shards() -> ShadowShards:
    global SHADOW_SHARDS

    if SHADOW_SHARDS is None:
        data_path = os.environ["DATA_PATH"].rstrip("/")
        SHADOW_SHARDS = ShadowShards(folder=f"{data_path}/shadow_shards")
    return SHADOW_SHARDS
