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

from nidx_protos import noderesources_pb2

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.index_message import IndexMessageBuilder
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos.writer_pb2 import BrokerMessage
from tests.ingest.fixtures import create_resource


async def test_generate_index_message_bw_compat(
    storage, maindb_driver: Driver, dummy_nidx_utility, knowledgebox_ingest: str
):
    full_resource = await create_resource(storage, maindb_driver, knowledgebox_ingest)

    async with maindb_driver.transaction(read_only=True) as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
        resource = await kb_obj.get(full_resource.uuid)
        assert resource is not None

        # Make sure that both the old and new way of generating index messages are equivalent
        imv1 = (await resource.generate_index_message(reindex=True)).brain
        imv2 = await IndexMessageBuilder(resource).full(reindex=True)

        # Relation fields to remove is not fully implemented in v1, so we skip it from the comparison
        imv1.ClearField("relation_fields_to_delete")
        imv2.ClearField("relation_fields_to_delete")

        # field_relations needs special treatment because it is a map of repeated fields
        assert flatten_field_relations(imv1) == flatten_field_relations(imv2)
        imv1.ClearField("field_relations")
        imv2.ClearField("field_relations")

        assert imv1 == imv2


def flatten_field_relations(r: noderesources_pb2.Resource):
    result = set()
    for field, field_relations in r.field_relations.items():
        for field_relation in field_relations.relations:
            to = field_relation.relation.to
            source = field_relation.relation.source
            result.add(
                (
                    field,
                    field_relation.relation.relation,
                    (
                        to.ntype,
                        to.subtype,
                        to.value,
                    ),
                    (
                        source.ntype,
                        source.subtype,
                        source.value,
                    ),
                )
            )


async def test_for_writer_bm_with_prefilter_update(
    storage, maindb_driver: Driver, dummy_nidx_utility, knowledgebox_ingest: str
):
    full_resource = await create_resource(storage, maindb_driver, knowledgebox_ingest)

    async with maindb_driver.transaction(read_only=True) as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
        resource = await kb_obj.get(full_resource.uuid)
        assert resource is not None
        writer_bm = BrokerMessage(
            kbid=knowledgebox_ingest,
            uuid=resource.uuid,
            reindex=True,
        )

        im = await IndexMessageBuilder(resource).for_writer_bm([writer_bm], resource_created=False)

        # title, summary, file, link, conv and text
        total_fields = 6

        # Only the texts and relations index is reindexed
        assert len(im.texts) == total_fields
        assert len(im.texts_to_delete) == total_fields
        assert len(im.field_relations) == 1  # a/metadata
        assert "a/metadata" in im.field_relations
        assert im.relation_fields_to_delete == ["a/metadata"]

        # Others are not
        assert len(im.paragraphs) == 0
        assert len(im.paragraphs_to_delete) == 0
        assert len(im.vector_prefixes_to_delete) == 0
