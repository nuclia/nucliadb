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
from typing import Any, Optional

import pytest

from nucliadb.common import datamanagers
from nucliadb.common.cluster import manager
from nucliadb.common.cluster.settings import settings
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import writer_pb2


def test_should_create_new_shard():
    sm = manager.KBShardManager()
    low_para_counter = {
        "num_paragraphs": settings.max_shard_paragraphs - 1,
    }
    high_para_counter = {
        "num_paragraphs": settings.max_shard_paragraphs + 1,
    }
    assert sm.should_create_new_shard(**low_para_counter) is False
    assert sm.should_create_new_shard(**high_para_counter) is True


async def test_shard_creation(dummy_nidx_utility, txn: Transaction):
    """Given a cluster of index nodes, validate shard creation logic.

    Every logic shard should create a configured amount of indexing replicas and
    update the information about writable shards.

    """
    kbid = f"kbid:{test_shard_creation.__name__}"
    sm = manager.KBShardManager()

    # Fake KB shards instead of creating a KB to generate it
    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    await datamanagers.cluster.update_kb_shards(
        txn,
        kbid=kbid,
        shards=writer_pb2.Shards(
            kbid=kbid,
        ),
    )

    # create first shard
    await sm.create_shard_by_kbid(txn, kbid)

    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    assert shards is not None
    assert len(shards.shards) == 1
    assert shards.shards[0].read_only is False
    # B/c with Shards.actual
    assert shards.actual == 0

    # adding a second shard will mark the first as read only
    await sm.create_shard_by_kbid(txn, kbid)

    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    assert shards is not None
    assert len(shards.shards) == 2
    assert shards.shards[0].read_only is True
    assert shards.shards[1].read_only is False
    # B/c with Shards.actual
    assert shards.actual == 1

    # adding a third one will be equivalent
    await sm.create_shard_by_kbid(txn, kbid)

    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    assert shards is not None
    assert len(shards.shards) == 3
    assert shards.shards[0].read_only is True
    assert shards.shards[1].read_only is True
    assert shards.shards[2].read_only is False
    # B/c with Shards.actual
    assert shards.actual == 2


@pytest.fixture
def txn():
    class MockTransaction:
        def __init__(self):
            self.store = {}

        async def get(self, key: str, for_update=False) -> Optional[Any]:
            return self.store.get(key, None)

        async def set(self, key: str, value: Any):
            self.store[key] = value

    yield MockTransaction()
