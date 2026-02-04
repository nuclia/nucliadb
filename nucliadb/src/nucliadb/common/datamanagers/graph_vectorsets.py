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

from nucliadb.common.datamanagers.utils import get_kv_pb
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import knowledgebox_pb2

KB_VECTORSETS_NODE = "/kbs/{kbid}/vectorsets_node"
KB_VECTORSETS_EDGE = "/kbs/{kbid}/vectorsets_edge"


class GraphVectorsetManager:
    def __init__(self, key: str):
        self.key = key

    async def get_all(self, txn: Transaction, *, kbid: str) -> list[knowledgebox_pb2.VectorSetConfig]:
        stored = await get_kv_pb(
            txn,
            self.kbid_key(kbid),
            knowledgebox_pb2.KnowledgeBoxVectorSetsConfig,
        )
        if stored:
            return [vs for vs in stored.vectorsets]
        else:
            return []

    async def set_all(
        self, txn: Transaction, *, kbid: str, configs: list[knowledgebox_pb2.VectorSetConfig]
    ):
        """Create or update a vectorset configuration"""
        data = knowledgebox_pb2.KnowledgeBoxVectorSetsConfig()
        data.vectorsets.extend(configs)

        await txn.set(self.kbid_key(kbid), data.SerializeToString())

    def kbid_key(self, kbid: str) -> str:
        return self.key.format(kbid=kbid)


node = GraphVectorsetManager(KB_VECTORSETS_NODE)
edge = GraphVectorsetManager(KB_VECTORSETS_EDGE)
