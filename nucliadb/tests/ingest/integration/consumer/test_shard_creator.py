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

import asyncio

import pytest

from nucliadb.ingest.consumer import shard_creator
from nucliadb_protos import writer_pb2
from nucliadb_utils import const

pytestmark = pytest.mark.asyncio


async def test_shard_auto_create(
    maindb_driver,
    pubsub,
    storage,
    fake_node,
    knowledgebox_ingest,
):
    from nucliadb.common.cluster.settings import settings

    settings.max_shard_paragraphs = 1

    sc = shard_creator.ShardCreatorHandler(
        driver=maindb_driver,
        storage=storage,
        pubsub=pubsub,
        check_delay=0.05,
    )
    await sc.initialize()

    original_kb_shards = await sc.shard_manager.get_shards_by_kbid_inner(
        knowledgebox_ingest
    )

    await pubsub.publish(
        const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=knowledgebox_ingest),
        writer_pb2.Notification(
            kbid=knowledgebox_ingest,
            action=writer_pb2.Notification.Action.INDEXED,
        ).SerializeToString(),
    )

    await asyncio.sleep(0.2)

    await sc.finalize()

    kb_shards = await sc.shard_manager.get_shards_by_kbid_inner(knowledgebox_ingest)
    assert len(kb_shards.shards) == len(original_kb_shards.shards) + 1
