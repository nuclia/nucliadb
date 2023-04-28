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
from nucliadb_protos.writer_pb2 import ShardObject as PBShard
from nucliadb_protos.writer_pb2 import ShardReplica as PBReplica

from nucliadb.ingest.orm.shard import Shard


def test_indexing_replicas():
    pbshard = PBShard()

    rep1 = PBReplica()
    rep1.shard.id = "rep1"
    rep1.node = "node1"
    pbshard.replicas.append(rep1)

    rep2 = PBReplica()
    rep2.shard.id = "rep2"
    rep2.node = "node2"
    pbshard.replicas.append(rep2)

    shard = Shard("shard_id", pbshard)

    assert shard.indexing_replicas() == [("rep1", "node1"), ("rep2", "node2")]
