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
from nucliadb_protos.nodesidecar_pb2 import Counter

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.cluster.settings import settings


def test_should_create_new_shard():
    sm = KBShardManager()
    low_para_counter = Counter(paragraphs=settings.max_shard_paragraphs - 1)
    high_para_counter = Counter(paragraphs=settings.max_shard_paragraphs + 1)
    assert sm.should_create_new_shard(low_para_counter) is False
    assert sm.should_create_new_shard(high_para_counter) is True

    low_fields_counter = Counter(fields=settings.max_shard_fields - 1)
    high_fields_counter = Counter(fields=settings.max_shard_fields + 1)
    assert sm.should_create_new_shard(low_fields_counter) is False
    assert sm.should_create_new_shard(high_fields_counter) is True
