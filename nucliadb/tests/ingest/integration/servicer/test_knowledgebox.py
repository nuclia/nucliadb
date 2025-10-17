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
from uuid import uuid4

import pytest

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.nidx import NidxUtility
from nucliadb_protos import knowledgebox_pb2, writer_pb2
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.mark.parametrize("prewarm", [True, False])
@pytest.mark.deploy_modes("component")
async def test_create_and_update_knowledgebox_with_prewarm(
    maindb_driver: Driver,
    dummy_nidx_utility: NidxUtility,
    nucliadb_ingest_grpc: WriterStub,
    hosted_nucliadb,
    prewarm: bool,
):
    kbid = str(uuid4())
    slug = f"slug-{kbid}"

    result = await nucliadb_ingest_grpc.NewKnowledgeBoxV2(  # type: ignore
        writer_pb2.NewKnowledgeBoxV2Request(
            kbid=kbid,
            slug=slug,
            title="My premium KB with prewarming",
            vectorsets=[
                writer_pb2.NewKnowledgeBoxV2Request.VectorSet(
                    vectorset_id="my-semantic-model",
                    vector_dimension=1024,
                )
            ],
            prewarm_enabled=prewarm,
        )
    )
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    async with maindb_driver.ro_transaction() as txn:
        config = await datamanagers.kb.get_config(txn, kbid=kbid)
        assert config is not None
        assert config.prewarm_enabled == prewarm

    # validate pre-warm flag is also forwarded to nidx
    assert dummy_nidx_utility.api_client.NewShard.call_count == 1
    assert dummy_nidx_utility.api_client.NewShard.call_args.args[0].prewarm_enabled == prewarm

    # now update multiple times the KB and check the flag is properly updated
    # everywhere

    for updated_prewarm in [not prewarm, not prewarm, prewarm, prewarm]:
        result = await nucliadb_ingest_grpc.UpdateKnowledgeBox(  # type: ignore
            writer_pb2.KnowledgeBoxUpdate(
                uuid=kbid,
                slug=slug,
                config=knowledgebox_pb2.KnowledgeBoxConfig(
                    prewarm_enabled=updated_prewarm,
                ),
            )
        )
        assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

        async with maindb_driver.ro_transaction() as txn:
            config = await datamanagers.kb.get_config(txn, kbid=kbid)
            assert config is not None
            assert config.prewarm_enabled == updated_prewarm

    # validate pre-warm flag is also forwarded to nidx, only on configuration changes
    assert dummy_nidx_utility.api_client.ConfigureShards.call_count == 2
    calls = dummy_nidx_utility.api_client.ConfigureShards.call_args_list

    for shard_config in calls[0].args[0].configs:
        assert shard_config.prewarm_enabled == (not prewarm)

    for shard_config in calls[1].args[0].configs:
        assert shard_config.prewarm_enabled == prewarm
