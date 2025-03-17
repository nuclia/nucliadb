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

from nucliadb.ingest import SERVICE_NAME
from nucliadb.ingest.orm.brain import relation_to_index
from nucliadb_protos.resources_pb2 import (
    Classification,
    FieldComputedMetadataWrapper,
    FieldEntity,
    FieldID,
    FieldText,
    FieldType,
)
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.utilities import get_storage


async def test_ingest_relations_indexing(
    dummy_nidx_utility, local_files, storage, knowledgebox_ingest, processor
):
    rid = str(uuid.uuid4())
    bm = BrokerMessage(kbid=knowledgebox_ingest, uuid=rid, slug="slug-1", type=BrokerMessage.AUTOCOMMIT)

    e0 = RelationNode(value="E0", ntype=RelationNode.NodeType.ENTITY, subtype="")
    e1 = RelationNode(value="E1", ntype=RelationNode.NodeType.ENTITY, subtype="Official")
    e2 = RelationNode(value="E2", ntype=RelationNode.NodeType.ENTITY, subtype="Propaganda")
    r0 = Relation(relation=Relation.RelationType.CHILD, source=e1, to=e2, relation_label="R0")
    r1 = Relation(relation=Relation.RelationType.ENTITY, source=e0, to=e2, relation_label="R1")
    r2 = Relation(relation=Relation.RelationType.CHILD, source=e0, to=e1, relation_label="R2")

    bm.relations.extend([r0, r1, r2])

    await processor.process(message=bm, seqid=1)

    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(dummy_nidx_utility.index.mock_calls[0][1][0])

    assert len(pb.relations) == 3
    assert pb.relations[0] == relation_to_index(r0)
    assert pb.relations[1] == relation_to_index(r1)
    assert pb.relations[2] == relation_to_index(r2)


async def test_ingest_label_relation_extraction(
    dummy_nidx_utility, local_files, storage, knowledgebox_ingest, processor
):
    rid = str(uuid.uuid4())
    bm = BrokerMessage(kbid=knowledgebox_ingest, uuid=rid, slug="slug-1", type=BrokerMessage.AUTOCOMMIT)

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

    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(dummy_nidx_utility.index.mock_calls[0][1][0])

    for i, (labelset, label) in enumerate(labels):
        assert pb.relations[i].relation == Relation.RelationType.ABOUT
        assert pb.relations[i].source.value == rid
        assert pb.relations[i].to.value == f"{labelset}/{label}"


async def test_ingest_colab_relation_extraction(
    dummy_nidx_utility, local_files, storage, knowledgebox_ingest, processor
):
    rid = str(uuid.uuid4())
    bm = BrokerMessage(kbid=knowledgebox_ingest, uuid=rid, slug="slug-1", type=BrokerMessage.AUTOCOMMIT)

    collaborators = ["Alice", "Bob", "Trudy"]
    bm.origin.colaborators.extend(collaborators)

    await processor.process(message=bm, seqid=1)

    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(dummy_nidx_utility.index.mock_calls[0][1][0])

    for i, collaborator in enumerate(collaborators):
        assert pb.relations[i].relation == Relation.RelationType.COLAB
        assert pb.relations[i].source.value == rid
        assert pb.relations[i].to.value == collaborator


async def test_ingest_field_metadata_relation_extraction(
    dummy_nidx_utility, local_files, storage, knowledgebox_ingest, processor
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
    # Data Augmentation + Processor entities
    fcmw.metadata.metadata.entities["my-task-id"].entities.extend(
        [
            FieldEntity(text="value-3", label="subtype-3"),
            FieldEntity(text="value-4", label="subtype-4"),
        ]
    )
    # Legacy processor entities
    # TODO: Remove once processor doesn't use this anymore and remove the positions and ner fields from the message
    fcmw.metadata.metadata.positions["subtype-1/value-1"].entity = "value-1"
    fcmw.metadata.metadata.positions["subtype-1/value-2"].entity = "value-2"

    fcmw.metadata.metadata.classifications.extend(
        [
            Classification(labelset="ls1", label="label1"),
        ]
    )

    bm.field_metadata.append(fcmw)

    await processor.process(message=bm, seqid=1)

    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(dummy_nidx_utility.index.mock_calls[0][1][0])

    generated_relations = [
        # From data augmentation + processor metadata
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE),
            to=RelationNode(value="value-3", ntype=RelationNode.NodeType.ENTITY, subtype="subtype-3"),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE),
            to=RelationNode(value="value-4", ntype=RelationNode.NodeType.ENTITY, subtype="subtype-4"),
        ),
        # From legacy ner metadata
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE),
            to=RelationNode(value="value-1", ntype=RelationNode.NodeType.ENTITY, subtype="subtype-1"),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE),
            to=RelationNode(value="value-2", ntype=RelationNode.NodeType.ENTITY, subtype="subtype-1"),
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
        assert relation_to_index(generated_relation) in pb.relations


async def test_ingest_field_relations_relation_extraction(
    dummy_nidx_utility, local_files, storage, knowledgebox_ingest, processor
):
    rid = str(uuid.uuid4())
    bm = BrokerMessage(kbid=knowledgebox_ingest, uuid=rid, slug="slug-1", type=BrokerMessage.AUTOCOMMIT)

    relationnode = RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE, subtype="subtype-1")
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

    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(dummy_nidx_utility.index.mock_calls[0][1][0])

    assert len(pb.relations) == len(test_relations)
    for relation in test_relations:
        assert relation_to_index(relation) in pb.relations
