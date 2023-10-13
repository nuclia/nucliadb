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
import pytest

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.settings import settings


def test_should_create_new_shard():
    sm = manager.KBShardManager()
    low_para_counter = {
        "num_paragraphs": settings.max_shard_paragraphs - 1,
        "num_fields": 0,
    }
    high_para_counter = {
        "num_paragraphs": settings.max_shard_paragraphs + 1,
        "num_fields": 0,
    }
    assert sm.should_create_new_shard(**low_para_counter) is False
    assert sm.should_create_new_shard(**high_para_counter) is True

    low_fields_counter = {"num_fields": settings.max_shard_fields, "num_paragraphs": 0}
    high_fields_counter = {
        "num_fields": settings.max_shard_fields + 1,
        "num_paragraphs": 0,
    }
    assert sm.should_create_new_shard(**low_fields_counter) is False
    assert sm.should_create_new_shard(**high_fields_counter) is True


@pytest.fixture(scope="function")
async def fake_node():
    manager.INDEX_NODES.clear()
    yield manager.add_index_node(
        id="node-0",
        address="nohost",
        shard_count=0,
        dummy=True,
    )
    manager.INDEX_NODES.clear()
