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
import contextlib
from typing import Optional

from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb_protos import knowledgebox_pb2, writer_pb2
from nucliadb_utils.keys import KB_SHARDS, KB_UUID


class KnowledgeBoxDataManager:
    def __init__(self, driver: Driver, *, read_only_txn: Optional[Transaction] = None):
        self.driver = driver
        self._read_only_txn = read_only_txn

    @contextlib.asynccontextmanager
    async def read_only_transaction(self):
        if self._read_only_txn is not None:
            yield self._read_only_txn
        else:
            async with self.driver.transaction(read_only=True) as txn:
                yield txn

    async def exists_kb(self, kbid: str) -> bool:
        return await self.get_config(kbid) is not None

    async def get_config(
        self, kbid: str
    ) -> Optional[knowledgebox_pb2.KnowledgeBoxConfig]:
        async with self.read_only_transaction() as txn:
            key = KB_UUID.format(kbid=kbid)
            payload = await txn.get(key)
            if payload is None:
                return None
            response = knowledgebox_pb2.KnowledgeBoxConfig()
            response.ParseFromString(payload)
            return response

    async def get_shards_object(self, kbid: str) -> writer_pb2.Shards:
        key = KB_SHARDS.format(kbid=kbid)
        async with self.read_only_transaction() as txn:
            payload = await txn.get(key)
            if not payload:
                raise ShardsNotFound(kbid)
            pb = writer_pb2.Shards()
            pb.ParseFromString(payload)
            return pb

    async def get_model_metadata(
        self, kbid: str
    ) -> knowledgebox_pb2.SemanticModelMetadata:
        try:
            shards_obj = await self.get_shards_object(kbid)
        except ShardsNotFound:
            raise KnowledgeBoxNotFound(kbid)
        if shards_obj.HasField("model"):
            return shards_obj.model
        else:
            # B/c code for old KBs that do not have the `model` attribute set in the Shards object.
            # Cleanup this code after a migration is done unifying all fields under `model` (on-prem and cloud).
            return knowledgebox_pb2.SemanticModelMetadata(
                similarity_function=shards_obj.similarity
            )
