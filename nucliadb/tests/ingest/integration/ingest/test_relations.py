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
import uuid

import pytest
from nucliadb_protos.resources_pb2 import (
    Classification,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldText,
    FieldType,
)
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest import SERVICE_NAME
from nucliadb_utils.utilities import get_indexing, get_storage


@pytest.mark.asyncio
async def test_ingest_relations_indexing(
    fake_node, local_files, storage, knowledgebox_ingest, processor
):
    rid = str(uuid.uuid4())
    bm = BrokerMessage(
        kbid=knowledgebox_ingest, uuid=rid, slug="slug-1", type=BrokerMessage.AUTOCOMMIT
    )

    e0 = RelationNode(value="E0", ntype=RelationNode.NodeType.ENTITY, subtype="")
    e1 = RelationNode(
        value="E1", ntype=RelationNode.NodeType.ENTITY, subtype="Official"
    )
    e2 = RelationNode(
        value="E2", ntype=RelationNode.NodeType.ENTITY, subtype="Propaganda"
    )
    r0 = Relation(
        relation=Relation.RelationType.CHILD, source=e1, to=e2, relation_label="R0"
    )
    r1 = Relation(
        relation=Relation.RelationType.ENTITY, source=e0, to=e2, relation_label="R1"
    )
    r2 = Relation(
        relation=Relation.RelationType.CHILD, source=e0, to=e1, relation_label="R2"
    )

    bm.relations.extend([r0, r1, r2])

    await processor.process(message=bm, seqid=1)

    index = get_indexing()
    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(index._calls[0][1])

    assert len(pb.relations) == 3
    assert pb.relations[0] == r0
    assert pb.relations[1] == r1
    assert pb.relations[2] == r2


@pytest.mark.asyncio
async def test_ingest_label_relation_extraction(
    fake_node, local_files, storage, knowledgebox_ingest, processor
):
    rid = str(uuid.uuid4())
    bm = BrokerMessage(
        kbid=knowledgebox_ingest, uuid=rid, slug="slug-1", type=BrokerMessage.AUTOCOMMIT
    )

    labels = [
        ("labelset-1", "label-1"),
        ("labelset-1", "label-2"),
        ("labelset-2", "label-1"),
        ("labelset-2", "label-3"),
    ]
    bm.basic.usermetadata.classifications.extend(
        [Classification(labelset=labelset, label=label) for labelset, label in labels]
    )

    await processor.process(message=bm, seqid=1)

    index = get_indexing()
    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(index._calls[0][1])

    for i, (labelset, label) in enumerate(labels):
        assert pb.relations[i].relation == Relation.RelationType.ABOUT
        assert pb.relations[i].source.value == rid
        assert pb.relations[i].to.value == f"{labelset}/{label}"


@pytest.mark.asyncio
async def test_ingest_colab_relation_extraction(
    fake_node, local_files, storage, knowledgebox_ingest, processor
):
    rid = str(uuid.uuid4())
    bm = BrokerMessage(
        kbid=knowledgebox_ingest, uuid=rid, slug="slug-1", type=BrokerMessage.AUTOCOMMIT
    )

    collaborators = ["Alice", "Bob", "Trudy"]
    bm.origin.colaborators.extend(collaborators)

    await processor.process(message=bm, seqid=1)

    index = get_indexing()
    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(index._calls[0][1])

    for i, collaborator in enumerate(collaborators):
        assert pb.relations[i].relation == Relation.RelationType.COLAB
        assert pb.relations[i].source.value == rid
        assert pb.relations[i].to.value == collaborator


@pytest.mark.asyncio
async def test_ingest_field_metadata_relation_extraction(
    fake_node, local_files, storage, knowledgebox_ingest, processor
):
    rid = str(uuid.uuid4())
    bm = BrokerMessage(
        kbid=knowledgebox_ingest,
        uuid=rid,
        slug="slug-1",
        type=BrokerMessage.AUTOCOMMIT,
        texts={
            "title": FieldText(
                body="Title with metadata",
                format=FieldText.Format.PLAIN,
            )
        },
    )

    fcmw = FieldComputedMetadataWrapper(
        field=FieldID(
            field_type=FieldType.TEXT,
            field="title",
        )
    )
    fcmw.metadata.metadata.positions["subtype-1/value-1"].entity = "value-1"
    fcmw.metadata.metadata.positions["subtype-1/value-2"].entity = "value-2"

    fcmw.metadata.metadata.classifications.extend(
        [
            Classification(labelset="ls1", label="label1"),
        ]
    )

    bm.field_metadata.append(fcmw)

    await processor.process(message=bm, seqid=1)

    index = get_indexing()
    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(index._calls[0][1])

    generated_relations = [
        # From ner metadata
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE),
            to=RelationNode(
                value="value-1", ntype=RelationNode.NodeType.ENTITY, subtype="subtype-1"
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE),
            to=RelationNode(
                value="value-2", ntype=RelationNode.NodeType.ENTITY, subtype="subtype-1"
            ),
        ),
        # From classification metadata
        Relation(
            relation=Relation.RelationType.ABOUT,
            source=RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE),
            to=RelationNode(
                value="ls1/label1",
                ntype=RelationNode.NodeType.LABEL,
            ),
        ),
    ]
    for generated_relation in generated_relations:
        assert generated_relation in pb.relations


@pytest.mark.asyncio
async def test_ingest_field_relations_relation_extraction(
    fake_node, local_files, storage, knowledgebox_ingest, processor
):
    rid = str(uuid.uuid4())
    bm = BrokerMessage(
        kbid=knowledgebox_ingest, uuid=rid, slug="slug-1", type=BrokerMessage.AUTOCOMMIT
    )

    relationnode = RelationNode(
        value=rid, ntype=RelationNode.NodeType.RESOURCE, subtype="subtype-1"
    )
    test_relations = [
        Relation(
            relation=Relation.RelationType.CHILD,
            source=relationnode,
            to=RelationNode(
                value="document",
                ntype=RelationNode.NodeType.RESOURCE,
            ),
        ),
        Relation(
            relation=Relation.RelationType.ABOUT,
            source=relationnode,
            to=RelationNode(
                value="label",
                ntype=RelationNode.NodeType.LABEL,
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relationnode,
            to=RelationNode(
                value="entity",
                ntype=RelationNode.NodeType.ENTITY,
            ),
        ),
        Relation(
            relation=Relation.RelationType.COLAB,
            source=relationnode,
            to=RelationNode(
                value="user",
                ntype=RelationNode.NodeType.USER,
            ),
        ),
        Relation(
            relation=Relation.RelationType.OTHER,
            source=relationnode,
            to=RelationNode(
                value="other",
                ntype=RelationNode.NodeType.RESOURCE,
            ),
        ),
    ]
    bm.relations.extend(test_relations)

    await processor.process(message=bm, seqid=1)

    index = get_indexing()
    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(index._calls[0][1])

    assert len(pb.relations) == len(test_relations)
    for relation in test_relations:
        assert relation in pb.relations
