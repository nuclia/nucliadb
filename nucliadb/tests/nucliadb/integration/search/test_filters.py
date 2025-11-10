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
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from nucliadb.common.cluster import rollover
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.utils import get_processor_bm, get_writer_bm
from nucliadb.search.search.rank_fusion import ReciprocalRankFusion
from nucliadb.tests.vectors import V1, V2, Q
from nucliadb_models.labels import Label, LabelSetKind
from nucliadb_models.search import FindOptions, MinScore, RerankerName
from nucliadb_protos.resources_pb2 import (
    Classification,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldEntity,
    FieldID,
    FieldType,
    Paragraph,
    ParagraphAnnotation,
    Position,
    UserFieldMetadata,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import broker_resource, inject_message
from tests.utils.dirty_index import mark_dirty


class ClassificationLabels:
    RESOURCE_ANNOTATED = "user-resource/label"
    PARAGRAPH_ANNOTATED = "user-paragraph/label"
    FIELD_DETECTED = "field/label"
    PARAGRAPH_DETECTED = "paragraph/label"


class EntityLabels:
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
    pos = Position(start=60, end=64)
    # Data Augmentation + Processor entities
    fmw.metadata.metadata.entities["my-task-id"].entities.extend(
        [
            FieldEntity(text=entity, label=family, positions=[pos]),
        ]
    )

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
    p1vec.vector.extend(V1)
    p2vec = Vector()
    p2vec.start = p2vec.start_paragraph = par2.start
    p2vec.end = p2vec.end_paragraph = par2.end
    p2vec.vector.extend(V2)
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
    # XXX remove title and summary info, as the test don't take them into
    # account, but the rest is expected for indexing.....
    bm.basic.ClearField("fieldmetadata")
    bm.ClearField("field_metadata")
    bm.ClearField("extracted_text")

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
    p1vec.vector.extend(V1)

    p2vec = Vector()
    p2vec.start = p2vec.start_paragraph = par2.start
    p2vec.end = p2vec.end_paragraph = par2.end
    p2vec.start_paragraph
    p2vec.vector.extend(V2)
    evw.vectors.vectors.vectors.append(p1vec)
    evw.vectors.vectors.vectors.append(p2vec)
    bm.field_vectors.append(evw)

    return bm


async def create_test_labelsets(nucliadb_writer: AsyncClient, kbid: str):
    for kind, _label in (
        (LabelSetKind.RESOURCES, ClassificationLabels.RESOURCE_ANNOTATED),
        (LabelSetKind.RESOURCES, ClassificationLabels.FIELD_DETECTED),
        (LabelSetKind.PARAGRAPHS, ClassificationLabels.PARAGRAPH_DETECTED),
        (LabelSetKind.PARAGRAPHS, ClassificationLabels.PARAGRAPH_ANNOTATED),
    ):
        labelset, label = _label.split("/")
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/labelset/{labelset}",
            json=dict(
                kind=[kind],
                labels=[Label(title=label).model_dump()],
            ),
        )
        assert resp.status_code == 200


@pytest.fixture(scope="function")
async def kbid(
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    await create_test_labelsets(nucliadb_writer, standalone_knowledgebox)
    bm_with_entities = broker_message_with_entities(standalone_knowledgebox)
    bm_with_labels = broker_message_with_labels(standalone_knowledgebox)
    for bm in (bm_with_entities, bm_with_labels):
        for partial_bm in (get_writer_bm(bm), get_processor_bm(bm)):
            await inject_message(nucliadb_ingest_grpc, partial_bm)
    return standalone_knowledgebox


@pytest.mark.deploy_modes("standalone")
async def test_filtering_before_and_after_reindexing(
    app_context, nucliadb_reader: AsyncClient, kbid: str
):
    FILTERS = [
        # Filter with unexisting labels and entities
        [entity_filter("unexisting/entity")],
        [label_filter("user-paragraph/unexisting")],
        [label_filter("paragraph/unexisting")],
        [label_filter("resource/unexisting")],
        [label_filter("user-resource/unexisting")],
        # Filter with existing labels and entities
        [entity_filter(EntityLabels.DETECTED)],
        [label_filter(ClassificationLabels.PARAGRAPH_ANNOTATED)],
        [label_filter(ClassificationLabels.PARAGRAPH_DETECTED)],
        [label_filter(ClassificationLabels.RESOURCE_ANNOTATED)],
        [label_filter(ClassificationLabels.FIELD_DETECTED)],
        # Combine filters
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
        [
            label_filter(ClassificationLabels.RESOURCE_ANNOTATED),
            label_filter(ClassificationLabels.PARAGRAPH_DETECTED),
        ],
    ]

    for f in FILTERS:
        await _test_filtering(nucliadb_reader, kbid, f)

    await rollover.rollover_kb_index(app_context, kbid)
    await mark_dirty()

    for f in FILTERS:
        await _test_filtering(nucliadb_reader, kbid, f)


def apply_old_filters(request, filters):
    request["filters"] = filters


def convert_filter(f):
    parts = f.split("/")
    if parts[1] == "entities":
        return {"prop": "entity", "subtype": parts[2], "value": parts[3]}
    else:
        return {"prop": "label", "labelset": parts[2], "label": parts[3]}


def apply_new_filters(request, filters):
    expr = {}
    request["filter_expression"] = expr
    paragraph_filters = [f for f in filters if "paragraph" in f]
    if paragraph_filters:
        expr["paragraph"] = {"and": [convert_filter(f) for f in paragraph_filters]}
    field_filters = [f for f in filters if "paragraph" not in f]
    if field_filters:
        expr["field"] = {"and": [convert_filter(f) for f in field_filters]}


async def _test_filtering(nucliadb_reader: AsyncClient, kbid: str, filters):
    # The set of expected results is always the AND of the filters
    filter_paragraphs = []
    expected_paragraphs = set()
    for fltr in filters:
        filter_paragraphs.append(FILTERS_TO_PARAGRAPHS.get(fltr, set()))
    expected_paragraphs = set.intersection(*filter_paragraphs)

    # Run tests with old filters and new filter_expression
    for apply_filters in [apply_old_filters, apply_new_filters]:
        with patch(
            "nucliadb.search.search.retrieval.get_rank_fusion",
            return_value=ReciprocalRankFusion(window=20),
        ):
            request = dict(
                query="",
                features=[FindOptions.KEYWORD, FindOptions.SEMANTIC],
                vector=Q,
                min_score=MinScore(semantic=-1).model_dump(),
                reranker=RerankerName.NOOP,
            )
            apply_filters(request, filters)
            resp = await nucliadb_reader.post(
                f"/kb/{kbid}/find",
                json=request,
            )
        assert resp.status_code == 200, resp.text
        content = resp.json()

        # Collect all paragraphs from the response
        paragraphs = [
            paragraph
            for resource in content["resources"].values()
            for field in resource["fields"].values()
            for paragraph in field["paragraphs"].values()
        ]

        # Check that only the expected paragraphs were returned
        assert len(paragraphs) == len(expected_paragraphs), (
            f"{filters}\n{paragraphs}\n{expected_paragraphs}"
        )
        not_yet_found = expected_paragraphs.copy()
        for par in paragraphs:
            if par["text"] not in expected_paragraphs:
                raise AssertionError(f"Paragraph not expected: {par['text']}")
            if par["text"] in not_yet_found:
                assert par["score_type"] == "BOTH", f"Score type not expected with filters {filters}"
                not_yet_found.remove(par["text"])
        assert len(not_yet_found) == 0, f"Some paragraphs were not found: {not_yet_found}"


@pytest.mark.deploy_modes("standalone")
async def test_filtering_field_and_paragraph(app_context, nucliadb_reader: AsyncClient, kbid: str):
    async def test_search(filter_expression, expected_paragraphs):
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json=dict(
                query="",
                features=[FindOptions.KEYWORD, FindOptions.SEMANTIC],
                vector=Q,
                min_score=MinScore(semantic=-1).model_dump(),
                reranker=RerankerName.NOOP,
                filter_expression=filter_expression,
            ),
        )
        assert resp.status_code == 200, resp.text
        content = resp.json()

        # Collect all paragraphs from the response
        paragraphs = [
            paragraph
            for resource in content["resources"].values()
            for field in resource["fields"].values()
            for paragraph in field["paragraphs"].values()
        ]
        print("found", paragraphs)
        print("expected", expected_paragraphs)

        # Check that only the expected paragraphs were returned
        assert len(paragraphs) == len(expected_paragraphs), (
            f"{filter_expression}\n{paragraphs}\n{expected_paragraphs}"
        )
        not_yet_found = expected_paragraphs.copy()
        for par in paragraphs:
            if par["text"] not in expected_paragraphs:
                raise AssertionError(f"Paragraph not expected: {par['text']}")
            if par["text"] in not_yet_found:
                assert par["score_type"] == "BOTH", (
                    f"Score type not expected with filters {filter_expression}"
                )
                not_yet_found.remove(par["text"])
        assert len(not_yet_found) == 0, f"Some paragraphs were not found: {not_yet_found}"

    resource_label = label_filter(ClassificationLabels.RESOURCE_ANNOTATED)
    paragraph_label = label_filter(ClassificationLabels.PARAGRAPH_ANNOTATED)
    await test_search(
        {
            "field": convert_filter(resource_label),
            "paragraph": convert_filter(paragraph_label),
        },
        set.intersection(FILTERS_TO_PARAGRAPHS[resource_label], FILTERS_TO_PARAGRAPHS[paragraph_label]),
    )

    await test_search(
        {
            "field": convert_filter(resource_label),
            "paragraph": convert_filter(paragraph_label),
            "operator": "or",
        },
        set.union(FILTERS_TO_PARAGRAPHS[resource_label], FILTERS_TO_PARAGRAPHS[paragraph_label]),
    )


@pytest.fixture()
async def app_context(natsd, storage, nucliadb):
    ctx = ApplicationContext()
    await ctx.initialize()
    yield ctx
    await ctx.finalize()
