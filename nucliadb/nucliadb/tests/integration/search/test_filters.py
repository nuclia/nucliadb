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
from httpx import AsyncClient
from nucliadb_protos.knowledgebox_pb2 import Label
from nucliadb_protos.resources_pb2 import (
    Classification,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
    ParagraphAnnotation,
    Position,
    TokenSplit,
    UserFieldMetadata,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import OpStatusWriter, SetLabelsRequest
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.common.cluster import rollover
from nucliadb.common.context import ApplicationContext
from nucliadb.tests.utils import broker_resource, inject_message
from nucliadb_models.labels import LabelSetKind
from nucliadb_models.search import MinScore
from nucliadb_protos import writer_pb2

RELEASE_CHANNELS = (
    "STABLE",
    "EXPERIMENTAL",
)


class ClassificationLabels:
    RESOURCE_ANNOTATED = "user-resource/label"
    PARAGRAPH_ANNOTATED = "user-paragraph/label"
    FIELD_DETECTED = "field/label"
    PARAGRAPH_DETECTED = "paragraph/label"


class EntityLabels:
    ANNOTATED = "PERSON/Rusty"
    DETECTED = "COUNTRY/Spain"


def entity_filter(entity):
    return f"/entities/{entity}"


def label_filter(label):
    return f"/classification.labels/{label}"


PARAGRAPH1 = "My name is Rusty and I am a rust software engineer based in Spain"
PARAGRAPH2 = "I think Python is cool too"
PARAGRAPH3 = "My name is Pietro and I am a Ruby developer based in Portugal"
PARAGRAPH4 = "In my free time, I code in C#."
ALL_PARAGRAPHS = {PARAGRAPH1, PARAGRAPH2, PARAGRAPH3, PARAGRAPH4}


FILTERS_TO_PARAGRAPHS = {
    entity_filter(EntityLabels.ANNOTATED): {PARAGRAPH1, PARAGRAPH2},
    entity_filter(EntityLabels.DETECTED): {PARAGRAPH1, PARAGRAPH2},
    label_filter(ClassificationLabels.RESOURCE_ANNOTATED): {PARAGRAPH3, PARAGRAPH4},
    label_filter(ClassificationLabels.FIELD_DETECTED): {PARAGRAPH3, PARAGRAPH4},
    label_filter(ClassificationLabels.PARAGRAPH_ANNOTATED): {PARAGRAPH3},
    label_filter(ClassificationLabels.PARAGRAPH_DETECTED): {PARAGRAPH4},
}


def broker_message_with_entities(kbid):
    """
    This simulates a broker messages coming from processing
    where an entity COUNTRY/Spain has been detected for a the field.
    """
    bm = broker_resource(kbid=kbid)
    field_id = "text"
    field = FieldID()
    field.field = field_id
    field.field_type = FieldType.TEXT

    # Add annotated entity
    ufm = UserFieldMetadata()
    ufm.field.CopyFrom(field)
    family, entity = EntityLabels.ANNOTATED.split("/")
    ufm.token.append(
        TokenSplit(
            token=entity, klass=family, start=11, end=16, cancelled_by_user=False
        )
    )
    bm.basic.fieldmetadata.append(ufm)

    # Add a couple of paragraphs to a text field
    p1 = PARAGRAPH1
    p2 = PARAGRAPH2
    text = "\n".join([p1, p2])

    bm.texts[field_id].body = text

    # Add extracted text
    etw = ExtractedTextWrapper()
    etw.body.text = text
    etw.field.CopyFrom(field)
    bm.extracted_text.append(etw)

    # Add field computed metadata with the detected entity
    fmw = FieldComputedMetadataWrapper()
    fmw.field.CopyFrom(field)
    family, entity = EntityLabels.DETECTED.split("/")
    fmw.metadata.metadata.ner[entity] = family
    pos = Position(start=60, end=64)
    fmw.metadata.metadata.positions[EntityLabels.DETECTED].position.append(pos)

    par1 = Paragraph()
    par1.start = 0
    par1.end = len(p1)
    par1.text = p1
    par2 = Paragraph()
    par2.start = par1.end + 1
    par2.end = par2.start + len(p2)
    par2.text = p2
    fmw.metadata.metadata.paragraphs.append(par1)
    fmw.metadata.metadata.paragraphs.append(par2)
    bm.field_metadata.append(fmw)

    # Add extracted vectors for the field
    evw = ExtractedVectorsWrapper()
    evw.field.CopyFrom(field)
    p1vec = Vector()
    p1vec.start = p1vec.start_paragraph = par1.start
    p1vec.end = p1vec.end_paragraph = par1.end
    p1vec.vector.extend([1, 1, 1])
    p2vec = Vector()
    p2vec.start = p2vec.start_paragraph = par2.start
    p2vec.end = p2vec.end_paragraph = par2.end
    p2vec.vector.extend([2, 2, 2])
    evw.vectors.vectors.vectors.append(p1vec)
    evw.vectors.vectors.vectors.append(p2vec)
    bm.field_vectors.append(evw)
    return bm


def broker_message_with_labels(kbid):
    """
    This simulates a broker messages with some labels at every
    level (resource, field and paragraph).
    """
    bm = broker_resource(kbid=kbid)
    field_id = "text"
    field = FieldID()
    field.field = field_id
    field.field_type = FieldType.TEXT

    text = "\n".join([PARAGRAPH3, PARAGRAPH4])

    bm.texts[field_id].body = text

    # Add extracted text
    etw = ExtractedTextWrapper()
    etw.body.text = text
    etw.field.CopyFrom(field)
    bm.extracted_text.append(etw)

    # Add a paragraph label
    ufm = UserFieldMetadata()
    ufm.field.CopyFrom(field)
    panno1 = ParagraphAnnotation(key=f"{bm.uuid}/t/text/0-{len(PARAGRAPH3)}")
    labelset, label = ClassificationLabels.PARAGRAPH_ANNOTATED.split("/")
    panno1.classifications.append(
        Classification(
            labelset=labelset,
            label=label,
            cancelled_by_user=False,
        )
    )
    ufm.paragraphs.append(panno1)
    bm.basic.fieldmetadata.append(ufm)

    # Add a resource label
    labelset, label = ClassificationLabels.RESOURCE_ANNOTATED.split("/")
    bm.basic.usermetadata.classifications.append(
        Classification(
            labelset=labelset,
            label=label,
            cancelled_by_user=False,
        )
    )

    # Add a classification label at the field level
    fmw = FieldComputedMetadataWrapper()
    fmw.field.CopyFrom(field)
    par1 = Paragraph()
    par1.start = 0
    par1.end = len(PARAGRAPH3)
    par1.text = PARAGRAPH3
    fmw.metadata.metadata.paragraphs.append(par1)

    par2 = Paragraph()
    par2.start = par1.end + 1
    par2.end = par2.start + len(PARAGRAPH4)
    par2.text = PARAGRAPH4

    # Add a detected classification label at the paragraph level
    labelset, label = ClassificationLabels.PARAGRAPH_DETECTED.split("/")
    par2.classifications.append(
        Classification(
            labelset=labelset,
            label=label,
        )
    )
    fmw.metadata.metadata.paragraphs.append(par2)
    # Add a detected classification label at the field level
    labelset, label = ClassificationLabels.FIELD_DETECTED.split("/")
    fmw.metadata.metadata.classifications.append(
        Classification(
            labelset=labelset,
            label=label,
        )
    )
    bm.field_metadata.append(fmw)

    # Add extracted vectors for the field
    evw = ExtractedVectorsWrapper()
    evw.field.CopyFrom(field)
    p1vec = Vector()
    p1vec.start = p1vec.start_paragraph = par1.start
    p1vec.end = p1vec.end_paragraph = par1.end
    p1vec.vector.extend([1, 1, 1])

    p2vec = Vector()
    p2vec.start = p2vec.start_paragraph = par2.start
    p2vec.end = p2vec.end_paragraph = par2.end
    p2vec.start_paragraph
    p2vec.vector.extend([2, 2, 2])
    evw.vectors.vectors.vectors.append(p1vec)
    evw.vectors.vectors.vectors.append(p2vec)
    bm.field_vectors.append(evw)

    return bm


async def create_test_labelsets(nucliadb_grpc, kbid: str):
    for kind, _label in (
        (LabelSetKind.RESOURCES, ClassificationLabels.RESOURCE_ANNOTATED),
        (LabelSetKind.RESOURCES, ClassificationLabels.FIELD_DETECTED),
        (LabelSetKind.PARAGRAPHS, ClassificationLabels.PARAGRAPH_DETECTED),
        (LabelSetKind.PARAGRAPHS, ClassificationLabels.PARAGRAPH_ANNOTATED),
    ):
        labelset, label = _label.split("/")
        req = set_labelset_request(
            kbid,
            labelset,
            kind=kind,
            labels=[Label(title=label)],
        )
        resp = await nucliadb_grpc.SetLabels(req)
        assert resp.status == OpStatusWriter.OK


def set_labelset_request(
    kbid, labelset_id, kind, labels: list[Label]
) -> writer_pb2.SetLabelsRequest:
    req = SetLabelsRequest()
    req.kb.uuid = kbid
    req.id = labelset_id
    req.labelset.multiple = True
    req.labelset.kind.append(kind)
    req.labelset.labels.extend(labels)
    return req


@pytest.fixture(scope="function")
async def kbid(
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    await create_test_labelsets(nucliadb_grpc, knowledgebox)
    await inject_message(nucliadb_grpc, broker_message_with_entities(knowledgebox))
    await inject_message(nucliadb_grpc, broker_message_with_labels(knowledgebox))
    return knowledgebox


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", RELEASE_CHANNELS, indirect=True)
@pytest.mark.parametrize(
    "filters",
    [
        # Filter with unexisting labels and entities
        [entity_filter("unexisting/entity")],
        [label_filter("user-paragraph/unexisting")],
        [label_filter("paragraph/unexisting")],
        [label_filter("resource/unexisting")],
        [label_filter("user-resource/unexisting")],
        # Filter with existing labels and entities
        [entity_filter(EntityLabels.ANNOTATED)],
        [entity_filter(EntityLabels.DETECTED)],
        [label_filter(ClassificationLabels.PARAGRAPH_ANNOTATED)],
        [label_filter(ClassificationLabels.PARAGRAPH_DETECTED)],
        [label_filter(ClassificationLabels.RESOURCE_ANNOTATED)],
        [label_filter(ClassificationLabels.FIELD_DETECTED)],
        # Combine filters
        [entity_filter(EntityLabels.ANNOTATED), entity_filter("unexisting/entity")],
        [entity_filter(EntityLabels.ANNOTATED), entity_filter(EntityLabels.DETECTED)],
        [
            entity_filter(EntityLabels.ANNOTATED),
            label_filter(ClassificationLabels.PARAGRAPH_DETECTED),
        ],
        [
            label_filter(ClassificationLabels.PARAGRAPH_ANNOTATED),
            label_filter("user-paragraph/unexisting"),
        ],
        [
            label_filter(ClassificationLabels.FIELD_DETECTED),
            label_filter("resource/unexisting"),
        ],
        [
            label_filter(ClassificationLabels.PARAGRAPH_ANNOTATED),
            label_filter(ClassificationLabels.PARAGRAPH_DETECTED),
        ],
        [
            label_filter(ClassificationLabels.RESOURCE_ANNOTATED),
            label_filter(ClassificationLabels.FIELD_DETECTED),
        ],
    ],
)
async def test_filtering_before_and_after_reindexing(
    app_context, nucliadb_reader: AsyncClient, kbid: str, filters
):
    await _test_filtering(nucliadb_reader, kbid, filters)

    await rollover.rollover_kb_shards(app_context, kbid)

    await _test_filtering(nucliadb_reader, kbid, filters)


async def _test_filtering(nucliadb_reader: AsyncClient, kbid: str, filters):
    # The set of expected results is always the AND of the filters
    filter_paragraphs = []
    expected_paragraphs = set()
    for fltr in filters:
        filter_paragraphs.append(FILTERS_TO_PARAGRAPHS.get(fltr, set()))
    expected_paragraphs = set.intersection(*filter_paragraphs)

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json=dict(
            query="",
            filters=filters,
            features=["paragraph", "vector"],
            vector=[0.5, 0.5, 0.5],
            min_score=MinScore(semantic=-1).dict(),
        ),
        timeout=None,
    )
    assert resp.status_code == 200
    content = resp.json()

    # Collect all paragraphs from the response
    paragraphs = [
        paragraph
        for resource in content["resources"].values()
        for field in resource["fields"].values()
        for paragraph in field["paragraphs"].values()
    ]

    # Check that only the expected paragraphs were returned
    assert len(paragraphs) == len(expected_paragraphs)
    not_yet_found = expected_paragraphs.copy()
    for par in paragraphs:
        if par["text"] not in expected_paragraphs:
            raise AssertionError(f"Paragraph not expected: {par['text']}")
        if par["text"] in not_yet_found:
            assert (
                par["score_type"] == "BOTH"
            ), f"Score type not expected with filters {filters}"
            not_yet_found.remove(par["text"])
    assert len(not_yet_found) == 0, f"Some paragraphs were not found: {not_yet_found}"


@pytest.fixture()
async def app_context(natsd, storage, nucliadb):
    ctx = ApplicationContext()
    await ctx.initialize()
    yield ctx
    await ctx.finalize()
