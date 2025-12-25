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
import base64
import json
from typing import Optional, cast
from unittest.mock import AsyncMock, patch

import pydantic
import pytest
from httpx import AsyncClient

from nucliadb.ingest.processing import DummyProcessingEngine, PushPayload
from nucliadb.learning_proxy import (
    LearningConfiguration,
    SemanticConfig,
    SimilarityFunction,
)
from nucliadb.models.internal.processing import ClassificationLabel
from nucliadb.writer.utilities import get_processing
from nucliadb_models import common, metadata
from nucliadb_models.resource import Resource
from nucliadb_models.search import KnowledgeboxCounters, KnowledgeboxSearchResults
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos import writer_pb2 as wpb
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet
from nucliadb_protos.resources_pb2 import (
    Answers,
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldQuestionAnswerWrapper,
    FieldType,
    FileExtractedData,
    LinkExtractedData,
    Paragraph,
    QuestionAnswer,
    Vector,
)
from nucliadb_protos.train_pb2 import GetSentencesRequest, TrainParagraph
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import broker_resource, inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import mark_dirty, wait_for_sync
from tests.writer.test_fields import (
    TEST_CONVERSATION_PAYLOAD,
    TEST_FILE_PAYLOAD,
    TEST_LINK_PAYLOAD,
    TEST_TEXT_PAYLOAD,
)


@pytest.mark.deploy_modes("standalone")
async def test_kb_creation_allows_setting_learning_configuration(
    nucliadb_writer_manager: AsyncClient,
    nucliadb_reader: AsyncClient,
    onprem_nucliadb,
):
    with patch("nucliadb.writer.api.v1.knowledgebox.learning_proxy", new=AsyncMock()) as learning_proxy:
        # We set this to None to test the case where the user has not
        # defined a learning configuration yet before creating the KB.
        learning_proxy.get_configuration.return_value = None
        learning_proxy.set_configuration.return_value = LearningConfiguration(
            semantic_model="english",
            semantic_vector_similarity="cosine",
            semantic_vector_size=384,
            semantic_model_configs={
                "english": SemanticConfig(
                    similarity=SimilarityFunction.COSINE,
                    size=384,
                    threshold=0.7,
                )
            },
        )

        # Check that we can define it to a different semantic model
        resp = await nucliadb_writer_manager.post(
            f"/kbs",
            json={
                "title": "My KB with english semantic model",
                "slug": "english",
                "learning_configuration": {"semantic_model": "english"},
            },
        )
        assert resp.status_code == 201
        kbid = resp.json()["uuid"]

        learning_proxy.set_configuration.assert_called_once_with(
            kbid, config={"semantic_models": ["english"]}
        )


@pytest.mark.deploy_modes("standalone")
async def test_creation(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_train_grpc: TrainStub,
    standalone_knowledgebox,
):
    # PUBLIC API
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}")
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/labelset/ls1",
        json={"title": "Labelset 1", "labels": [{"text": "text", "title": "title"}]},
    )
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
        },
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # PROCESSING API

    bm = BrokerMessage()
    et = ExtractedTextWrapper()
    fm = FieldComputedMetadataWrapper()
    et.field.field = "text1"
    fm.field.field = "text1"
    et.field.field_type = FieldType.TEXT
    fm.field.field_type = FieldType.TEXT
    et.body.text = "My text"
    fm.metadata.metadata.language = "en"
    p1 = Paragraph()
    p1.start = 0
    p1.end = 7

    fm.metadata.metadata.paragraphs.append(p1)
    bm.extracted_text.append(et)
    bm.field_metadata.append(fm)
    bm.uuid = rid
    bm.kbid = standalone_knowledgebox

    await inject_message(nucliadb_ingest_grpc, bm)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=extracted&show=values&extracted=text&extracted=metadata",
    )
    assert resp.status_code == 200
    assert (
        resp.json()["data"]["texts"]["text1"]["extracted"]["metadata"]["metadata"]["paragraphs"][0][
            "end"
        ]
        == 7
    )

    # ADD A LABEL

    resp = await nucliadb_writer.patch(
        f"/kb/{standalone_knowledgebox}/resource/{rid}",
        json={
            "fieldmetadata": [
                {
                    "field": {
                        "field": "text1",
                        "field_type": "text",
                    },
                    "paragraphs": [
                        {
                            "key": f"{rid}/t/text1/0-7",
                            "classifications": [{"labelset": "ls1", "label": "label"}],
                        }
                    ],
                }
            ]
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=errors&show=values&show=basic",
    )
    assert resp.status_code == 200

    # TRAINING GRPC API
    request = GetSentencesRequest()
    request.kb.uuid = standalone_knowledgebox
    request.metadata.labels = True
    request.metadata.text = True
    paragraph: TrainParagraph
    async for paragraph in nucliadb_train_grpc.GetParagraphs(request):  # type: ignore
        if paragraph.field.field == "title":
            assert paragraph.metadata.text == "My title"
        else:
            assert paragraph.metadata.text == "My text"
            assert paragraph.metadata.labels.paragraph[0].label == "label"

    # TRAINING REST API
    trainset = TrainSet()
    trainset.batch_size = 20
    trainset.type = TaskType.PARAGRAPH_CLASSIFICATION
    trainset.filter.labels.append("ls1")
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/trainset")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["partitions"]) == 1
    partition_id = data["partitions"][0]

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/trainset/{partition_id}",
        content=trainset.SerializeToString(),
    )
    assert len(resp.content) > 0


@pytest.mark.deploy_modes("standalone")
async def test_can_create_standalone_knowledgebox_with_colon_in_slug(
    nucliadb_writer_manager: AsyncClient, nucliadb_reader_manager: AsyncClient
):
    resp = await nucliadb_writer_manager.post("/kbs", json={"slug": "something:else"})
    assert resp.status_code == 201

    resp = await nucliadb_reader_manager.get(f"/kbs")
    assert resp.status_code == 200
    assert resp.json()["kbs"][0]["slug"] == "something:else"


@pytest.mark.deploy_modes("standalone")
async def test_serialize_errors(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    """
    Test description:

    - Create a bm with errors for every type of field
    - Get the resource and check that error serialization works
    """

    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My resource",
            "texts": {"text": TEST_TEXT_PAYLOAD},
            "links": {"link": TEST_LINK_PAYLOAD},
            "files": {"file": TEST_FILE_PAYLOAD},
            "conversations": {"conversation": TEST_CONVERSATION_PAYLOAD},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    br = broker_resource(standalone_knowledgebox, rid=rid)

    # Add an error for every field type
    fields_to_test = [
        (rpb.FieldType.TEXT, "text", "texts"),
        (rpb.FieldType.FILE, "file", "files"),
        (rpb.FieldType.LINK, "link", "links"),
        (rpb.FieldType.CONVERSATION, "conversation", "conversations"),
    ]
    for ftype, fid, _ in fields_to_test:
        field = rpb.FieldID(field_type=ftype, field=fid)
        fcmw = FieldComputedMetadataWrapper()
        fcmw.field.CopyFrom(field)
        fcmw.metadata.metadata.language = "es"
        br.field_metadata.append(fcmw)
        error = wpb.Error(
            field=field.field,
            field_type=field.field_type,
            error="Failed",
            code=wpb.Error.ErrorCode.EXTRACT,
        )
        br.errors.append(error)

    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}",
        params=dict(show=["extracted", "errors", "basic"], extracted=["metadata"]),
    )
    assert resp.status_code == 200
    resp_json = resp.json()

    for _, fid, ftypestring in fields_to_test:
        assert resp_json["data"][ftypestring][fid]["error"]["body"] == "Failed"
        assert resp_json["data"][ftypestring][fid]["error"]["code"] == 1


@pytest.mark.deploy_modes("standalone")
async def test_extracted_shortened_metadata(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    """
    Test description:

    - Create a resource with a field containing FieldMetadata with ner, positions and relations.
    - Check that new extracted data option filters them out
    """
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My resource",
            "texts": {"text": {"body": "My text"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    br = broker_resource(
        standalone_knowledgebox,
        rid=rid,
        slug="myresource",
    )

    field = rpb.FieldID(field_type=rpb.FieldType.TEXT, field="text")
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(field)
    fcmw.metadata.metadata.language = "es"

    # Add some relations
    relation = rpb.Relation(
        source=rpb.RelationNode(value="abc"), to=rpb.RelationNode(value="xyz"), relation_label="foo"
    )
    relations = rpb.Relations()
    relations.relations.append(relation)
    fcmw.metadata.metadata.relations.append(relations)
    fcmw.metadata.split_metadata["split"].relations.append(relations)

    # Add some ners with position
    fcmw.metadata.metadata.entities["processor"].entities.extend(
        [
            rpb.FieldEntity(text="Barcelona", label="CITY", positions=[rpb.Position(start=1, end=2)]),
        ]
    )
    fcmw.metadata.split_metadata["split"].entities["processor"].entities.extend(
        [
            rpb.FieldEntity(text="Barcelona", label="CITY", positions=[rpb.Position(start=1, end=2)]),
        ]
    )

    # Add some classification
    classification = rpb.Classification(label="foo", labelset="bar")
    fcmw.metadata.metadata.classifications.append(classification)
    fcmw.metadata.split_metadata["split"].classifications.append(classification)

    br.field_metadata.append(fcmw)

    await inject_message(nucliadb_ingest_grpc, br)

    # TODO: Remove ner and positions once fields are removed
    cropped_fields = ["ner", "positions", "relations", "classifications"]

    # Check that when 'shortened_metadata' in extracted param fields are cropped
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}/text/text",
        params=dict(show=["extracted"], extracted=["shortened_metadata"]),
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    metadata = resp_json["extracted"]["metadata"]["metadata"]
    split_metadata = resp_json["extracted"]["metadata"]["split_metadata"]["split"]
    for meta in (metadata, split_metadata):
        for cropped_field in cropped_fields:
            assert len(meta[cropped_field]) == 0

    # Check that when 'metadata' in extracted param fields are returned
    for extracted_param in (["metadata"], ["metadata", "shortened_metadata"]):
        resp = await nucliadb_reader.get(
            f"/kb/{standalone_knowledgebox}/resource/{br.uuid}/text/text",
            params=dict(show=["extracted"], extracted=extracted_param),
        )
        assert resp.status_code == 200
        resp_json = resp.json()
        metadata = resp_json["extracted"]["metadata"]["metadata"]
        split_metadata = resp_json["extracted"]["metadata"]["split_metadata"]["split"]
        for meta in (metadata, split_metadata):
            for cropped_field in cropped_fields:
                assert len(meta[cropped_field]) > 0


@pytest.mark.parametrize(
    "field_id,error",
    [
        ("foobar", False),
        ("My_Field_1", False),
        ("With Spaces Not Allowed", True),
        ("Invalid&Character", True),
    ],
)
@pytest.mark.deploy_modes("standalone")
async def test_field_ids_are_validated(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    field_id,
    error,
):
    payload = {
        "title": "Foo",
        "texts": {
            field_id: {
                "format": "HTML",
                "body": "<p>whatever</p>",
            }
        },
    }
    resp = await nucliadb_writer.post(f"/kb/{standalone_knowledgebox}/resources", json=payload)
    if error:
        assert resp.status_code == 422
        body = resp.json()
        assert body["detail"][0]["type"] == "value_error"
    else:
        assert resp.status_code == 201


@pytest.mark.deploy_modes("standalone")
async def test_extra(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    """
    Test description:
    - Check that limits are applied
    - Check that it is returned only if requested on resource GET
    - Check that it is returned only if requested on search results
    - Check modification
    """
    kbid = standalone_knowledgebox
    invalid_extra = {"metadata": {i: f"foo{i}" for i in range(100000)}}
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Foo",
            "extra": invalid_extra,
        },
    )
    assert resp.status_code == 422
    error_detail = resp.json()["detail"][0]
    assert error_detail["loc"] == ["body", "extra"]
    assert error_detail["type"] == "value_error"
    assert "metadata should be less than 400000 bytes when serialized to JSON" in error_detail["msg"]
    extra = {
        "metadata": {
            "str": "str",
            "number": 2.0,
            "list": [1.0, 2.0, 3.0],
            "dict": {"foo": "bar"},
        }
    }
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Foo",
            "extra": extra,
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Check that extra metadata is not returned by default on GET
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}")
    assert resp.status_code == 200
    assert "extra" not in resp.json()

    # Check that extra metadata is returned when requested on GET
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}?show=extra")
    assert resp.status_code == 200
    assert resp.json()["extra"] == extra

    # Check that extra metadata is not returned by default on search
    resp = await nucliadb_reader.get(f"/kb/{kbid}/search?query=foo")
    assert resp.status_code == 200
    resource = resp.json()["resources"][rid]
    assert "extra" not in resource

    # Check that extra metadata is returned when requested on search results
    resp = await nucliadb_reader.get(f"/kb/{kbid}/search?query=foo&show=extra")
    assert resp.status_code == 200
    resource = resp.json()["resources"][rid]
    assert resource["extra"] == extra

    # Check modification of extra metadata
    extra["metadata"].pop("dict")
    resp = await nucliadb_writer.patch(f"/kb/{kbid}/resource/{rid}", json={"extra": extra})
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}?show=extra")
    assert resp.status_code == 200
    assert resp.json()["extra"] == extra


@pytest.mark.deploy_modes("standalone")
async def test_icon_doesnt_change_after_labeling_resource_sc_5625(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={"title": "Foo", "icon": "application/pdf"},
    )
    assert resp.status_code == 201
    uuid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{uuid}")
    assert resp.json()["icon"] == "application/pdf"

    # A partial patch should not change the icon
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{uuid}",
        json={"usermetadata": {"classifications": [{"labelset": "foo", "label": "bar"}]}},
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{uuid}")
    assert resp.json()["icon"] == "application/pdf"


@pytest.mark.parametrize(
    "slug,valid",
    [
        ("foo", True),
        ("foo-bar", True),  # with dash
        ("foo:bar", True),  # with colon
        ("foo_bar", True),  # with underscore
        ("FooBar", True),  # with capital letters
        ("foo.bar", False),  # with dot
        ("foo/bar", False),  # with slash
    ],
)
@pytest.mark.deploy_modes("standalone")
async def test_resource_slug_validation(
    nucliadb_writer, nucliadb_reader: AsyncClient, standalone_knowledgebox, slug, valid
):
    resp = await nucliadb_writer.post(f"/kb/{standalone_knowledgebox}/resources", json={"slug": slug})
    if valid:
        assert resp.status_code == 201
        resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/slug/{slug}")
        assert resp.status_code == 200
    else:
        assert resp.status_code == 422
        detail = resp.json()["detail"][0]
        assert detail["loc"] == ["body", "slug"]
        assert f"Invalid slug: '{slug}'" in detail["msg"]


@pytest.mark.deploy_modes("standalone")
async def test_icon_doesnt_change_after_adding_file_field_sc_2388(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Foo",
            "icon": "text/plain",
            "texts": {"text": {"body": "my text"}},
        },
    )
    assert resp.status_code == 201
    uuid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{uuid}")
    assert resp.json()["icon"] == "text/plain"

    # A subsequent file upload should not change the icon
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resource/{uuid}/file/file/upload",
        content=b"foo" * 200,
    )
    assert resp.status_code == 201

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{uuid}")
    assert resp.json()["icon"] == "text/plain"


@pytest.mark.deploy_modes("standalone")
async def test_language_metadata(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My resource",
            "texts": {"text": {"body": "My text"}},
        },
    )
    assert resp.status_code == 201
    uuid = resp.json()["uuid"]

    # Detected language in processing should be stored in basic metadata
    bm = BrokerMessage()
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.kbid = kbid
    bm.uuid = uuid
    field = FieldID(field_type=FieldType.TEXT, field="text")

    led = LinkExtractedData()
    led.field = field.field
    led.language = "ca"
    bm.link_extracted_data.append(led)

    fed = FileExtractedData()
    fed.field = field.field
    fed.language = "es"
    bm.file_extracted_data.append(fed)

    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(field)
    fcmw.metadata.metadata.language = "en"
    fcmw.metadata.split_metadata["foo"].language = "it"
    bm.field_metadata.append(fcmw)

    await inject_message(nucliadb_ingest_grpc, bm)

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{uuid}", params={"show": ["basic"]})
    assert resp.status_code == 200
    res = resp.json()
    assert res["metadata"]["language"] == "ca"
    assert set(res["metadata"]["languages"]) == {"ca", "es", "it", "en"}

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={"metadata": {"language": "en"}},
    )
    assert resp.status_code == 201
    uuid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{uuid}", params={"show": ["basic"]})
    assert resp.status_code == 200
    res = resp.json()
    assert res["metadata"]["language"] == "en"
    assert res["metadata"]["languages"] == []

    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{uuid}",
        json={"metadata": {"language": "de"}},
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{uuid}", params={"show": ["basic"]})
    assert resp.status_code == 200
    res = resp.json()
    assert res["metadata"]["language"] == "de"
    assert res["metadata"]["languages"] == []


@pytest.mark.deploy_modes("standalone")
async def test_story_7081(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
        },
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_writer.patch(
        f"/kb/{standalone_knowledgebox}/resource/{rid}",
        json={"origin": {"metadata": {"some": "data"}}},
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=origin",
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["origin"]["metadata"]["some"] == "data"


@pytest.mark.deploy_modes("standalone")
async def test_question_answer(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    # create a new resource
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
        },
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # send a broker message with the Q/A
    message = BrokerMessage()
    qaw = FieldQuestionAnswerWrapper()
    qaw.field.field_type = FieldType.TEXT
    qaw.field.field = "text1"

    for i in range(10):
        qa = QuestionAnswer()

        qa.question.text = f"My question {i}"
        qa.question.language = "catalan"
        qa.question.ids_paragraphs.extend([f"id1/{i}", f"id2/{i}"])

        for x in range(2):
            answer = Answers()
            answer.text = f"My answer {i}{x}"
            answer.language = "catalan"
            answer.ids_paragraphs.extend([f"id1/{i}{x}", f"id2/{i}{x}"])
            qa.answers.append(answer)

        qaw.question_answers.question_answers.question_answer.append(qa)

    message.question_answers.append(qaw)
    message.uuid = rid
    message.kbid = standalone_knowledgebox

    await inject_message(nucliadb_ingest_grpc, message)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=extracted&extracted=question_answers",
    )
    assert resp.status_code == 200
    data = resp.json()

    assert data["data"]["texts"]["text1"]["extracted"]["question_answers"]["question_answers"][
        "question_answer"
    ][0] == {
        "question": {
            "text": "My question 0",
            "language": "catalan",
            "ids_paragraphs": ["id1/0", "id2/0"],
        },
        "answers": [
            {
                "ids_paragraphs": ["id1/00", "id2/00"],
                "language": "catalan",
                "text": "My answer 00",
            },
            {
                "ids_paragraphs": ["id1/01", "id2/01"],
                "language": "catalan",
                "text": "My answer 01",
            },
        ],
    }

    # Test deletion of Q/A
    bm = BrokerMessage()
    bm.uuid = rid
    bm.kbid = standalone_knowledgebox
    field = FieldID()
    field.CopyFrom(qaw.field)
    bm.delete_question_answers.append(field)

    await inject_message(nucliadb_ingest_grpc, bm)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=extracted&extracted=question_answers",
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["data"]["texts"]["text1"]["extracted"].get("question_answers") is None


@pytest.mark.deploy_modes("standalone")
async def test_question_answer_annotations(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    qa_annotation = metadata.QuestionAnswerAnnotation(
        question_answer=common.QuestionAnswer(
            question=common.Question(
                text="My question 0",
                language="catalan",
                ids_paragraphs=["id1/0", "id2/0"],
            ),
            answers=[
                common.Answer(
                    ids_paragraphs=["id1/00", "id2/00"],
                    language="catalan",
                    text="My answer 00",
                )
            ],
        ),
        cancelled_by_user=True,
    )

    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
            "fieldmetadata": [
                {
                    "field": {
                        "field": "text1",
                        "field_type": "text",
                    },
                    "question_answers": [qa_annotation.model_dump()],
                }
            ],
        },
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=basic",
    )
    assert resp.status_code == 200
    data = resp.json()
    resource = Resource.model_validate(data)
    assert resource.fieldmetadata[0].question_answers[0] == qa_annotation  # type: ignore


@pytest.mark.deploy_modes("standalone")
async def test_link_fields_store_css_selector(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
            "links": {
                "link": {
                    "uri": "https://www.example.com",
                    "css_selector": "main",
                },
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=values",
    )
    assert resp.status_code == 200
    data = resp.json()
    resource = Resource.model_validate(data)
    css_selector = None
    if (
        resource.data is not None
        and resource.data.links is not None
        and resource.data.links["link"].value is not None
    ):
        css_selector = resource.data.links["link"].value.css_selector

    assert css_selector == "main"


@pytest.mark.deploy_modes("standalone")
async def test_link_fields_store_xpath(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
            "links": {
                "link": {
                    "uri": "https://www.example.com",
                    "xpath": "my_xpath",
                },
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=values",
    )
    assert resp.status_code == 200
    data = resp.json()
    resource = Resource.model_validate(data)
    xpath = None
    if (
        resource.data is not None
        and resource.data.links is not None
        and resource.data.links["link"].value is not None
    ):
        xpath = resource.data.links["link"].value.xpath

    assert xpath == "my_xpath"


@pytest.mark.deploy_modes("standalone")
async def test_dates_are_properly_validated(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My title",
            "origin": {
                "created": "0000-01-01T00:00:00Z",
            },
        },
    )
    assert resp.status_code == 422, print(resp.text)
    detail = resp.json()["detail"][0]
    assert detail["loc"] == ["body", "origin", "created"]

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My title",
            "origin": {
                "created": "0001-01-01T00:00:00Z",
            },
        },
    )
    assert resp.status_code == 201, print(resp.text)
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}?show=origin")
    assert resp.status_code == 200, resp.text

    assert resp.json()["origin"]["created"] == "0001-01-01T00:00:00Z"


@pytest.mark.deploy_modes("standalone")
async def test_file_computed_titles_are_set_on_resource_title(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    # Create a resource with an email field
    kbid = standalone_knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "my_email.eml",
            "files": {
                "email": {
                    "file": {
                        "filename": "my_email.eml",
                        "payload": base64.b64encode(b"email content").decode(),
                        "content_type": "message/rfc822",
                    }
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    # Simulate processing file extracted data with a computed title
    extracted_title = "Subject : My Email"
    bm = BrokerMessage()
    bm.type = BrokerMessage.MessageType.AUTOCOMMIT
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.uuid = rid
    bm.kbid = kbid
    fed = FileExtractedData()
    fed.field = "email"
    fed.title = extracted_title
    bm.file_extracted_data.append(fed)
    await inject_message(nucliadb_ingest_grpc, bm)

    # Check that the resource title changed
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}")
    assert resp.status_code == 200
    assert resp.json()["title"] == extracted_title

    # Now test that if the title is changed on creation, it is not overwritten
    kbid = standalone_knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Something else",
            "files": {
                "email": {
                    "file": {
                        "filename": "my_email.eml",
                        "payload": base64.b64encode(b"email content").decode(),
                        "content_type": "message/rfc822",
                    }
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid2 = resp.json()["uuid"]

    # Simulate processing file extracted data with a computed title
    extracted_title = "Foobar"
    bm = BrokerMessage()
    bm.type = BrokerMessage.MessageType.AUTOCOMMIT
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.uuid = rid2
    bm.kbid = kbid
    fed = FileExtractedData()
    fed.field = "email"
    fed.title = extracted_title
    bm.file_extracted_data.append(fed)
    await inject_message(nucliadb_ingest_grpc, bm)

    # Check that the resource title changed
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid2}")
    assert resp.status_code == 200
    title = resp.json()["title"]
    assert title != extracted_title
    assert title == "Something else"

    # Now check that if the reset_title flag is set on reprocess endpoint, the title is changed
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resource/{rid2}/reprocess",
        params={"reset_title": True},
    )
    assert resp.status_code == 202, resp.text

    bm.file_extracted_data[0].title = "Foobar (computed)"
    await inject_message(nucliadb_ingest_grpc, bm)

    # Check that the resource title changed
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid2}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "Foobar (computed)"


@pytest.mark.deploy_modes("standalone")
async def test_link_computed_titles_are_automatically_set_to_resource_title(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    # Create a resource with a link field (no title set)
    kbid = standalone_knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "link": {
                "mylink": {
                    "uri": "https://wikipedia.org/Lionel_Messi",
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    # Simulate processing link extracted data with a computed title
    extracted_title = "Lionel Messi - Wikipedia (computed)"
    bm = BrokerMessage()
    bm.type = BrokerMessage.MessageType.AUTOCOMMIT
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.uuid = rid
    bm.kbid = kbid
    bm.link_extracted_data.append(LinkExtractedData(field="mylink", title=extracted_title))
    await inject_message(nucliadb_ingest_grpc, bm)

    # Check that the resource title changed
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}")
    assert resp.status_code == 200
    assert resp.json()["title"] == extracted_title

    # Now test that if the title is changed on creation, it is not overwritten
    kbid = standalone_knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Luis Suarez - Wikipedia",
            "link": {
                "mylink": {
                    "uri": "https://wikipedia.org/Luis_Suarez",
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid2 = resp.json()["uuid"]

    # Simulate processing link extracted data with a computed title
    extracted_title = "Luis Suarez - Wikipedia (computed)"
    bm = BrokerMessage()
    bm.type = BrokerMessage.MessageType.AUTOCOMMIT
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.uuid = rid2
    bm.kbid = kbid
    bm.link_extracted_data.append(
        LinkExtractedData(
            field="mylink",
            title=extracted_title,
        )
    )
    await inject_message(nucliadb_ingest_grpc, bm)

    # Check that the resource title changed
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid2}")
    assert resp.status_code == 200
    title = resp.json()["title"]
    assert title != extracted_title
    assert title == "Luis Suarez - Wikipedia"

    # Now check that if the reset_title flag is set on reprocess endpoint, the title is changed
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resource/{rid2}/reprocess",
        params={"reset_title": True},
    )
    assert resp.status_code == 202, resp.text

    # Simulate processing link extracted data with a computed title
    await inject_message(nucliadb_ingest_grpc, bm)

    # Check that the resource title changed
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid2}")
    assert resp.status_code == 200
    assert resp.json()["title"] == extracted_title

    # Make sure that a new reprocess without reset_title does not change the title
    bm.link_extracted_data[0].title = "Luis Suarez - Wikipedia (computed again)"
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resource/{rid2}/reprocess",
    )
    assert resp.status_code == 202, resp.text

    # Simulate processing link extracted data with a computed title
    await inject_message(nucliadb_ingest_grpc, bm)

    # Check that the resource title did not change
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid2}")
    assert resp.status_code == 200
    assert resp.json()["title"] == extracted_title


@pytest.mark.deploy_modes("standalone")
async def test_jsonl_text_field(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My title",
            "texts": {
                "jsonl": {
                    "body": "\n".join(
                        [
                            json.dumps({"text": "foo"}),
                            json.dumps({"text": "bar"}),
                        ]
                    ),
                    "format": "JSONL",
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(
        f"kb/{standalone_knowledgebox}/resource/{rid}?show=values&show=basic",
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    text_field_data = data["data"]["texts"]["jsonl"]["value"]
    assert text_field_data["format"] == "JSONL"
    assert text_field_data["body"] == "\n".join(
        [
            json.dumps({"text": "foo"}),
            json.dumps({"text": "bar"}),
        ]
    )
    assert data["icon"] == "application/x-ndjson"


@pytest.mark.deploy_modes("standalone")
async def test_extract_and_split_strategy_on_fields(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    processing = get_processing()
    assert isinstance(processing, DummyProcessingEngine)
    processing.calls.clear()
    processing.values.clear()

    # Create a resource with a field of each type
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My title",
            "texts": {
                "text": {
                    "body": "My text",
                    "extract_strategy": "foo",
                    "split_strategy": "foo_split",
                }
            },
            "links": {
                "link": {
                    "uri": "https://www.example.com",
                    "extract_strategy": "bar",
                    "split_strategy": "bar_split",
                }
            },
            "files": {
                "file": {
                    "language": "en",
                    "file": {
                        "filename": "my_file.pdf",
                        "payload": base64.b64encode(b"file content").decode(),
                        "content_type": "application/pdf",
                    },
                    "extract_strategy": "baz",
                    "split_strategy": "baz_split",
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    # Check that the extract strategies are stored
    resp = await nucliadb_reader.get(
        f"kb/{standalone_knowledgebox}/resource/{rid}?show=values",
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["data"]["texts"]["text"]["value"]["extract_strategy"] == "foo"
    assert data["data"]["links"]["link"]["value"]["extract_strategy"] == "bar"
    assert data["data"]["files"]["file"]["value"]["extract_strategy"] == "baz"

    assert data["data"]["texts"]["text"]["value"]["split_strategy"] == "foo_split"
    assert data["data"]["links"]["link"]["value"]["split_strategy"] == "bar_split"
    assert data["data"]["files"]["file"]["value"]["split_strategy"] == "baz_split"

    # Check that push payload has been sent to processing with the right extract strategies
    def validate_processing_call(processing: DummyProcessingEngine):
        assert len(processing.values["send_to_process"]) == 1
        send_to_process_call = processing.values["send_to_process"][0][0]
        assert send_to_process_call.textfield["text"].extract_strategy == "foo"
        assert send_to_process_call.linkfield["link"].extract_strategy == "bar"

        assert send_to_process_call.textfield["text"].split_strategy == "foo_split"
        assert send_to_process_call.linkfield["link"].split_strategy == "bar_split"
        assert len(send_to_process_call.filefield) == 1
        assert processing.values["convert_internal_filefield_to_str"][0][0].extract_strategy == "baz"
        assert processing.values["convert_internal_filefield_to_str"][0][0].split_strategy == "baz_split"

    validate_processing_call(processing)

    processing.calls.clear()
    processing.values.clear()

    # Reprocess resource should also send the extract strategies
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/resource/{rid}/reprocess",
    )
    assert resp.status_code == 202, resp.text

    validate_processing_call(processing)

    # Update them to make sure they are stored correctly
    resp = await nucliadb_writer.patch(
        f"kb/{standalone_knowledgebox}/resource/{rid}",
        json={
            "texts": {
                "text": {
                    "body": "My text",
                    "extract_strategy": "foo1",
                    "split_strategy": "foo1_split",
                }
            },
            "links": {
                "link": {
                    "uri": "https://www.example.com",
                    "extract_strategy": "bar1",
                    "split_strategy": "bar1_split",
                }
            },
            "files": {
                "file": {
                    "language": "en",
                    "file": {
                        "filename": "my_file.pdf",
                        "payload": base64.b64encode(b"file content").decode(),
                        "content_type": "application/pdf",
                    },
                    "extract_strategy": "baz1",
                    "split_strategy": "baz1_split",
                }
            },
        },
    )
    assert resp.status_code == 200, resp.text

    # Check that the extract strategies are stored
    resp = await nucliadb_reader.get(
        f"kb/{standalone_knowledgebox}/resource/{rid}?show=values",
    )
    assert resp.status_code == 200, resp.text

    data = resp.json()
    assert data["data"]["texts"]["text"]["value"]["extract_strategy"] == "foo1"
    assert data["data"]["links"]["link"]["value"]["extract_strategy"] == "bar1"
    assert data["data"]["files"]["file"]["value"]["extract_strategy"] == "baz1"

    assert data["data"]["texts"]["text"]["value"]["split_strategy"] == "foo1_split"
    assert data["data"]["links"]["link"]["value"]["split_strategy"] == "bar1_split"
    assert data["data"]["files"]["file"]["value"]["split_strategy"] == "baz1_split"

    processing.calls.clear()
    processing.values.clear()

    # Upload a file with the upload endpoint, and set the extract strategy via a header
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/resource/{rid}/file/file2/upload",
        headers={"x-extract-strategy": "barbafoo", "x-split-strategy": "barbafoo_split"},
        content=b"file content",
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    # Check that the extract strategy is stored
    resp = await nucliadb_reader.get(
        f"kb/{standalone_knowledgebox}/resource/{rid}?show=values",
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["data"]["files"]["file2"]["value"]["extract_strategy"] == "barbafoo"
    assert data["data"]["files"]["file2"]["value"]["split_strategy"] == "barbafoo_split"

    # Check processing
    assert len(processing.values["send_to_process"]) == 1
    send_to_process_call = processing.values["send_to_process"][0][0]
    assert len(send_to_process_call.filefield) == 1
    assert processing.values["convert_internal_filefield_to_str"][0][0].extract_strategy == "barbafoo"
    assert (
        processing.values["convert_internal_filefield_to_str"][0][0].split_strategy == "barbafoo_split"
    )

    processing.calls.clear()
    processing.values.clear()

    # Upload a file with the kb upload endpoint, and set the extract strategy via a header
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/upload",
        headers={"x-extract-strategy": "barbafoo", "x-split-strategy": "barbafoo_split"},
        content=b"file content",
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    # Check that the extract strategy is stored
    resp = await nucliadb_reader.get(
        f"kb/{standalone_knowledgebox}/resource/{rid}?show=values",
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    item_strategies = data["data"]["files"].popitem()
    assert item_strategies[1]["value"]["extract_strategy"] == "barbafoo"
    assert item_strategies[1]["value"]["split_strategy"] == "barbafoo_split"

    processing.calls.clear()
    processing.values.clear()

    # Upload a file with the tus upload endpoint, and set the extract strategy via a header
    def header_encode(some_string):
        return base64.b64encode(some_string.encode()).decode()

    encoded_filename = header_encode("image.jpeg")
    encoded_language = header_encode("ca")
    upload_metadata = ",".join(
        [
            f"filename {encoded_filename}",
            f"language {encoded_language}",
        ]
    )
    file_content = b"file content"
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/tusupload",
        headers={
            "x-extract-strategy": "barbafoo-tus",
            "x-split-strategy": "barbafoo-tus_split",
            "tus-resumable": "1.0.0",
            "upload-metadata": upload_metadata,
            "content-type": "image/jpeg",
            "upload-length": str(len(file_content)),
        },
    )
    assert resp.status_code == 201, resp.text
    url = resp.headers["location"]

    resp = await nucliadb_writer.patch(
        url,
        content=file_content,
        headers={
            "upload-offset": "0",
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.headers["Tus-Upload-Finished"] == "1"
    rid = resp.headers["NDB-Resource"].split("/")[-1]
    field_id = resp.headers["NDB-Field"].split("/")[-1]

    # Check that the extract strategy is stored
    resp = await nucliadb_reader.get(
        f"kb/{standalone_knowledgebox}/resource/{rid}?show=values",
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["data"]["files"][field_id]["value"]["extract_strategy"] == "barbafoo-tus"
    assert data["data"]["files"][field_id]["value"]["split_strategy"] == "barbafoo-tus_split"

    # Check processing
    assert len(processing.values["send_to_process"]) == 1
    send_to_process_call = processing.values["send_to_process"][0][0]
    assert len(send_to_process_call.filefield) == 1
    assert (
        processing.values["convert_internal_filefield_to_str"][0][0].extract_strategy == "barbafoo-tus"
    )
    assert (
        processing.values["convert_internal_filefield_to_str"][0][0].split_strategy
        == "barbafoo-tus_split"
    )


@pytest.mark.deploy_modes("standalone")
async def test_classification_labels_are_sent_to_processing(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    """
    We aim to test all the classification labels are sent to the processing engine request so that they
    can apply data augmentation agents depending on labels criteria. Tests this on:
    - Resource creation
    - Resource patch
    - File upload
    - Reprocess resource
    - Reprocess field
    - Tusupload with classification labels
    """

    processing = get_processing()
    assert isinstance(processing, DummyProcessingEngine)
    processing.calls.clear()
    processing.values.clear()
    expected_labels = [ClassificationLabel(labelset="foo", label="bar")]

    # Resource creation
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My title",
            "usermetadata": {"classifications": [expected_labels[0].model_dump()]},
            "texts": {
                "text": {
                    "body": "My text",
                }
            },
            "links": {
                "link": {
                    "uri": "https://www.example.com",
                }
            },
            "files": {
                "file": {
                    "language": "en",
                    "file": {
                        "filename": "my_file.pdf",
                        "payload": base64.b64encode(b"file content").decode(),
                        "content_type": "application/pdf",
                    },
                }
            },
            "conversations": {
                "conv": {
                    "messages": [
                        {
                            "ident": "1",
                            "content": {
                                "text": "My message",
                            },
                        }
                    ]
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    def validate_processing_call(processing: DummyProcessingEngine):
        assert len(processing.values["send_to_process"]) == 1
        send_to_process_call = cast(PushPayload, processing.values["send_to_process"][0][0])
        assert send_to_process_call.textfield.popitem()[1].classification_labels == expected_labels
        assert send_to_process_call.linkfield.popitem()[1].classification_labels == expected_labels
        assert (
            send_to_process_call.conversationfield.popitem()[1].classification_labels == expected_labels
        )
        assert processing.values["convert_internal_filefield_to_str"][0][-1] == expected_labels

    validate_processing_call(processing)

    processing.calls.clear()
    processing.values.clear()

    # Reprocess resource
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/resource/{rid}/reprocess",
    )
    assert resp.status_code == 202, resp.text

    validate_processing_call(processing)

    processing.calls.clear()
    processing.values.clear()

    # Resource patch
    resp = await nucliadb_writer.patch(
        f"kb/{standalone_knowledgebox}/resource/{rid}",
        json={
            "texts": {
                "text2": {
                    "body": "My text",
                }
            },
        },
    )
    assert resp.status_code == 200, resp.text

    assert len(processing.values["send_to_process"]) == 1
    send_to_process_call = cast(PushPayload, processing.values["send_to_process"][0][0])
    assert send_to_process_call.textfield["text2"].classification_labels == [
        ClassificationLabel(labelset="foo", label="bar")
    ]

    processing.calls.clear()
    processing.values.clear()

    # Field upload
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/resource/{rid}/file/file2/upload",
        headers={"x-extract-strategy": "barbafoo"},
        content=b"file content",
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    assert len(processing.values["send_to_process"]) == 1
    send_to_process_call = cast(PushPayload, processing.values["send_to_process"][0][0])
    assert len(send_to_process_call.filefield) == 1
    assert processing.values["convert_internal_filefield_to_str"][0][-1] == expected_labels

    processing.calls.clear()
    processing.values.clear()

    # Reprocess field
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/resource/{rid}/file/file2/reprocess",
    )
    assert resp.status_code == 202, resp.text

    assert len(processing.values["send_to_process"]) == 1
    send_to_process_call = cast(PushPayload, processing.values["send_to_process"][0][0])
    assert len(send_to_process_call.filefield) == 1
    assert processing.values["convert_internal_filefield_to_str"][0][-1] == expected_labels

    processing.calls.clear()
    processing.values.clear()

    # Tusupload with classification labels
    def header_encode(some_string):
        return base64.b64encode(some_string.encode()).decode()

    encoded_filename = header_encode("image.jpeg")
    encoded_language = header_encode("ca")
    upload_metadata = ",".join(
        [
            f"filename {encoded_filename}",
            f"language {encoded_language}",
        ]
    )
    file_content = b"file content"
    resp = await nucliadb_writer.post(
        f"kb/{standalone_knowledgebox}/tusupload",
        headers={
            "x-extract-strategy": "barbafoo-tus",
            "tus-resumable": "1.0.0",
            "upload-metadata": upload_metadata,
            "upload-length": str(len(file_content)),
        },
        json={"usermetadata": {"classifications": [{"labelset": "foo", "label": "bar"}]}},
    )
    assert resp.status_code == 201, resp.text
    url = resp.headers["location"]

    resp = await nucliadb_writer.patch(
        url,
        content=file_content,
        headers={
            "upload-offset": "0",
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.headers["Tus-Upload-Finished"] == "1"

    assert len(processing.values["send_to_process"]) == 1
    send_to_process_call = cast(PushPayload, processing.values["send_to_process"][0][0])
    assert len(send_to_process_call.filefield) == 1
    assert processing.values["convert_internal_filefield_to_str"][0][-1] == expected_labels


@pytest.mark.deploy_modes("standalone")
async def test_deletions_on_text_index(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    async def kb_search(query: str, filters: Optional[list[str]] = None):
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/search",
            json={
                "query": query,
                "features": ["fulltext"],
                "show": ["basic", "values"],
                "filters": filters or [],
            },
        )
        assert resp.status_code == 200
        results = KnowledgeboxSearchResults.model_validate(resp.json())
        return results

    async def kb_counters(kbid: str) -> KnowledgeboxCounters:
        # We call counters endpoint to get the sentences count only
        resp = await nucliadb_reader.get(f"/kb/{kbid}/counters")
        assert resp.status_code == 200, resp.text
        counters = KnowledgeboxCounters.model_validate(resp.json())

        # We don't call /counters endpoint purposefully, as deletions are not guaranteed to be merged yet.
        # Instead, we do some searches.
        resp = await nucliadb_reader.get(f"/kb/{kbid}/search", params={"show": ["basic", "values"]})
        assert resp.status_code == 200, resp.text
        search = KnowledgeboxSearchResults.model_validate(resp.json())
        # Update paragraphs and fulltext in the counters object
        counters.paragraphs = search.paragraphs.total  # type: ignore
        counters.fields = search.fulltext.total  # type: ignore
        return counters

    resp = await nucliadb_writer.post(
        f"kb/{kbid}/resources",
        json={
            "title": "My title",
            "texts": {
                "alfredo": {
                    "body": "My name is Alfredo",
                },
                "salvatore": {
                    "body": "My name is Salvatore",
                },
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]
    await mark_dirty()
    await wait_for_sync()

    results = await kb_search(query="Alfredo")
    assert results.resources[rid].data.texts["alfredo"] is not None  # type: ignore
    results = await kb_search(query="Salvatore")
    assert results.resources[rid].data.texts["salvatore"] is not None  # type: ignore
    counters = await kb_counters(kbid)
    assert counters.resources == 1
    assert counters.fields == 3  # the two fields plus the title

    # Delete a text field now
    resp = await nucliadb_writer.delete(
        f"kb/{kbid}/resource/{rid}/text/alfredo",
    )
    assert resp.status_code == 204, resp.text
    await mark_dirty()
    await wait_for_sync()

    results = await kb_search(query="My name is")
    assert results.fulltext.total == 1
    assert "alfredo" not in results.resources[rid].data.texts  # type: ignore
    assert "salvatore" in results.resources[rid].data.texts  # type: ignore
    counters = await kb_counters(kbid)
    assert counters.resources == 1
    assert counters.fields == 2  # the one field plus the title

    # Add another text field
    resp = await nucliadb_writer.patch(
        f"kb/{kbid}/resource/{rid}",
        json={
            "texts": {
                "popeye": {
                    "body": "My name is Popeye",
                }
            }
        },
    )
    assert resp.status_code == 200
    await mark_dirty()
    await wait_for_sync()

    results = await kb_search(query="My name is")
    assert results.fulltext.total == 2
    assert len(results.resources[rid].data.texts) == 2  # type: ignore
    assert results.resources[rid].data.texts["salvatore"] is not None  # type: ignore
    assert results.resources[rid].data.texts["popeye"] is not None  # type: ignore

    counters = await kb_counters(kbid)
    assert counters.resources == 1
    assert counters.fields == 3  # the two fields plus the title

    # Now relabel the resource -- the label should be propagated to all fields
    resp = await nucliadb_writer.patch(
        f"kb/{kbid}/resource/{rid}",
        json={"usermetadata": {"classifications": [{"labelset": "foo", "label": "bar"}]}},
    )
    assert resp.status_code == 200, resp.text
    await mark_dirty()
    await wait_for_sync()

    results = await kb_search(query="My name is", filters=["/classification.labels/foo/bar"])
    assert len(results.resources[rid].data.texts) == 2  # type: ignore
    assert results.resources[rid].data.texts["salvatore"] is not None  # type: ignore
    assert results.resources[rid].data.texts["popeye"] is not None  # type: ignore
    counters = await kb_counters(kbid)
    assert counters.resources == 1
    assert counters.fields == 3  # the two fields plus the title

    # Try now updating the title of the resource -- text field count should not increase
    resp = await nucliadb_writer.patch(
        f"kb/{kbid}/resource/{rid}",
        json={"title": "My new title is about Songoku"},
    )
    assert resp.status_code == 200, resp.text
    await mark_dirty()
    await wait_for_sync()

    results = await kb_search(query="Songoku")
    assert len(results.resources) == 1
    assert results.resources[rid].title == "My new title is about Songoku"
    counters = await kb_counters(kbid)
    assert counters.resources == 1
    assert counters.fields == 3  # the two fields plus the title

    # Check that updating the slug does not duplicate text fields on the index
    resp = await nucliadb_writer.patch(
        f"kb/{kbid}/resource/{rid}",
        json={"slug": "my-new-slug"},
    )
    assert resp.status_code == 200, resp.text
    await mark_dirty()
    await wait_for_sync()
    counters = await kb_counters(kbid)
    assert counters.resources == 1
    assert counters.fields == 3

    # Check that we don't duplicate fields when a broker message from the
    # semantic model migration worker is ingested
    bmb = BrokerMessageBuilder(kbid=kbid, rid=rid, source=BrokerMessage.MessageSource.PROCESSOR)
    fb = bmb.field_builder("title", field_type=FieldType.GENERIC)
    fb.with_extracted_vectors(
        vectors=[Vector(start=0, end=10, vector=[1.0 for i in range(1024)])], vectorset="multilingual"
    )
    bm = bmb.build()
    await inject_message(nucliadb_ingest_grpc, bm)

    await mark_dirty()
    await wait_for_sync()

    counters = await kb_counters(kbid)
    assert counters.resources == 1
    assert counters.fields == 3  # the two fields plus the title

    # Now delete the resource
    resp = await nucliadb_writer.delete(f"kb/{kbid}/resource/{rid}")
    assert resp.status_code == 204, resp.text
    await mark_dirty()
    await wait_for_sync()

    # Check that the resource is gone
    results = await kb_search(query="My name is")
    assert len(results.resources) == 0
    counters = await kb_counters(kbid)
    assert counters.resources == 0
    assert counters.fields == 0


async def test_origin_limits():
    # Too many tags
    metadata.InputOrigin(
        tags=[str(i) for i in range(300)],
    )
    with pytest.raises(pydantic.ValidationError):
        metadata.InputOrigin(
            tags=[str(i) for i in range(300 + 1)],
        )

    # Tag is too long
    metadata.InputOrigin(
        tags=["a" * 512],
    )
    with pytest.raises(pydantic.ValidationError):
        metadata.InputOrigin(
            tags=["a" * (512 + 1)],
        )


@pytest.mark.deploy_modes("standalone")
async def test_get_kb_by_slug_on_cloud(
    nucliadb_writer_manager: AsyncClient,
    nucliadb_reader: AsyncClient,
):
    account_id = "myaccount"
    user_slug = "english"
    resp = await nucliadb_writer_manager.post(
        f"/kbs",
        json={
            "title": "My KB",
            # We simulate what idp does with kb slugs so they can be reused on different accounts
            "slug": f"{account_id}:{user_slug}",
        },
    )
    assert resp.status_code == 201
    kbid = resp.json()["uuid"]

    # Check that passing hte account id as header (same like what happens on cloud deployments), the kb is found
    resp = await nucliadb_reader.get(f"/kb/s/{user_slug}", headers={"x-nucliadb-account": account_id})
    assert resp.status_code == 200
    assert resp.json()["uuid"] == kbid

    # Passing the full slug works too
    resp = await nucliadb_reader.get(f"/kb/s/{account_id}:{user_slug}")
    assert resp.status_code == 200
    assert resp.json()["uuid"] == kbid


@pytest.fixture()
def log_client_errors_envvar():
    from nucliadb_utils.settings import running_settings

    running_settings.debug = True


@pytest.mark.deploy_modes("standalone")
async def test_client_errors_can_be_logged_on_server_side(
    log_client_errors_envvar,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    with patch("nucliadb.middleware.logger") as middleware_logger:
        kbid = standalone_knowledgebox

        # Make a request that triggers a 422 error
        resp = await nucliadb_writer.post(f"/kb/{kbid}/resources", json={"title": 456})
        assert resp.status_code == 422
        assert resp.json()["detail"][0]["msg"] == "Input should be a valid string"
        assert resp.json()["detail"][0]["input"] == 456
        resp_bytes = resp.content.decode()

        # Check that the error was logged on server side
        middleware_logger.info.assert_called_once_with(
            f"Client error. Response payload: {resp_bytes}",
            extra={
                "request_method": "POST",
                "request_path": f"/api/v1/kb/{kbid}/resources",
                "response_status_code": 422,
            },
        )
