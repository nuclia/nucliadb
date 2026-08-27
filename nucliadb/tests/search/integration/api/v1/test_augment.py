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

import json
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.models.internal.augment import (
    Augmented,
    DeepResourceAugment,
    FieldAugment,
    FieldClassificationLabels,
    FieldEntities,
    FieldText,
    Paragraph,
    ParagraphAugment,
    ParagraphText,
    RelatedParagraphs,
    ResourceAugment,
    ResourceClassificationLabels,
    ResourceSummary,
    ResourceTitle,
)
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_models.augment import AugmentedFileField, AugmentResponse
from nucliadb_models.common import FieldTypeName
from nucliadb_models.filters import Field
from nucliadb_models.search import ResourceProperties
from nucliadb_protos.resources_pb2 import ExtractedTextWrapper, FieldID, FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import cookie_tale_resource, smb_wonder_resource
from tests.utils import inject_message


@pytest.mark.deploy_modes("standalone")
async def test_augment_api(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    rid = await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "resources": [
                {
                    "given": [rid],
                    "basic": True,
                }
            ],
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/smb-wonder/145-234"},
                    ],
                    "text": True,
                }
            ],
        },
    )
    assert resp.status_code == 200

    body = AugmentResponse.model_validate(resp.json())

    assert body.resources[rid].slug == "smb-wonder"
    assert body.resources[rid].title == "Super Mario Bros. Wonder"
    assert (
        body.paragraphs[f"{rid}/f/smb-wonder/145-234"].text
        == "As one of eight player characters, the player completes levels across the Flower Kingdom."
    )


@pytest.mark.deploy_modes("standalone")
async def test_augment_api_resource_fields(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    rid = await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    # Validate how the resource fields text is returned depending on the filters

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "resources": [
                {
                    "given": [rid],
                    "fields": {
                        "text": True,
                        # no filters means all resource fields
                        "filters": [],
                    },
                }
            ],
        },
    )
    assert resp.status_code == 200

    body = AugmentResponse.model_validate(resp.json())
    field = body.fields[f"{rid}/f/smb-wonder"]
    assert field.text is not None and len(field.text) == 234

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "resources": [
                {
                    "given": [rid],
                    "fields": {
                        "text": True,
                        "filters": [
                            # only text fields
                            {"prop": "field", "type": "file"}
                        ],
                    },
                }
            ],
        },
    )
    assert resp.status_code == 200

    body = AugmentResponse.model_validate(resp.json())
    field = body.fields[f"{rid}/f/smb-wonder"]
    assert field.text is not None and len(field.text) == 234

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "resources": [
                {
                    "given": [rid],
                    "fields": {
                        "text": True,
                        "filters": [
                            # try with all other field types
                            {"prop": "field", "type": t}
                            for t in ["text", "link", "generic", "conversation"]
                        ],
                    },
                }
            ],
        },
    )
    assert resp.status_code == 200

    body = AugmentResponse.model_validate(resp.json())
    # no field returned, as the resource only has a file field
    assert len(body.fields) == 0


@pytest.mark.deploy_modes("standalone")
async def test_augment_api_images(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    rid = await cookie_tale_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/cookie-recipie/0-29"},
                    ],
                    "source_image": True,
                }
            ],
        },
    )
    assert resp.status_code == 200
    body = AugmentResponse.model_validate(resp.json())
    assert body.paragraphs[f"{rid}/f/cookie-recipie/0-29"].source_image == "generated/cookies.png"

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/cookie-recipie/29-75"},
                    ],
                    "table_image": True,
                }
            ],
        },
    )
    assert resp.status_code == 200
    body = AugmentResponse.model_validate(resp.json())
    assert (
        body.paragraphs[f"{rid}/f/cookie-recipie/29-75"].table_image == "generated/ingredients_table.png"
    )

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/cookie-recipie/29-75"},
                    ],
                    "table_image": True,
                    "table_prefers_page_preview": True,
                }
            ],
        },
    )
    assert resp.status_code == 200
    body = AugmentResponse.model_validate(resp.json())
    assert (
        body.paragraphs[f"{rid}/f/cookie-recipie/29-75"].table_image
        == "generated/extracted_images_1.png"
    )

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "paragraphs": [
                {
                    "given": [
                        {"id": f"{rid}/f/cookie-recipie/29-75"},
                    ],
                    "page_preview_image": True,
                }
            ],
        },
    )
    assert resp.status_code == 200
    body = AugmentResponse.model_validate(resp.json())
    assert (
        body.paragraphs[f"{rid}/f/cookie-recipie/29-75"].page_preview_image
        == "generated/extracted_images_1.png"
    )


@pytest.mark.deploy_modes("standalone")
async def test_augment_api_file_thumbnails(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    rid = await cookie_tale_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "fields": [
                {
                    "given": [f"{rid}/f/cookie-recipie"],
                    "file_thumbnail": True,
                }
            ],
        },
    )
    assert resp.status_code == 200

    body: AugmentResponse = AugmentResponse.model_validate(resp.json())
    assert f"{rid}/f/cookie-recipie" in body.fields
    field = body.fields[f"{rid}/f/cookie-recipie"]
    assert isinstance(field, AugmentedFileField)
    assert field.thumbnail_image == "file_thumbnail"

    # the path returned can be used to download the thumbnail
    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/resource/{rid}/file/cookie-recipie/download/extracted/{field.thumbnail_image}"
    )
    assert resp.status_code == 200
    assert resp.content == b"cookie recipie (file) thumbnail"


@pytest.mark.deploy_modes("standalone")
async def test_augment_api_ask_compat(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    """/augment endpoint compatibility tests for /ask (now outside nucliadb)."""
    kbid = knowledgebox
    rid = await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    augmented = Augmented(resources={}, resources_deep={}, fields={}, paragraphs={})
    with patch("nucliadb.search.api.v1.augment.augmentor.augment", return_value=augmented) as augment:
        #
        # TEST STRATEGY: full resource
        # It augments resource title, summary and all it's fields text
        #
        resp = await nucliadb_search.post(
            f"/{KB_PREFIX}/{kbid}/augment",
            json={
                "resources": [
                    {
                        "given": [rid],
                        "title": True,
                        "summary": True,
                        "fields": {
                            "text": True,
                            # no filters means all resource fields
                            "filters": [],
                        },
                    }
                ],
            },
        )
        assert resp.status_code == 200
        assert augment.call_count == 1
        assert augment.call_args.args[0] == kbid
        assert augment.call_args.args[1] == [
            ResourceAugment(
                given=[rid],
                select=[
                    ResourceTitle(),
                    ResourceSummary(),
                ],
            ),
            FieldAugment(given=[rid], select=[FieldText()], filter=[]),
        ]
        augment.reset_mock()

        #
        # TEST STRATEGY: field extension
        # It augments resource title, summary and specific fields text
        #
        resp = await nucliadb_search.post(
            f"/{KB_PREFIX}/{kbid}/augment",
            json={
                "resources": [
                    {
                        "given": [rid],
                        "title": True,
                        "summary": True,
                        "fields": {
                            "text": True,
                            "filters": [
                                # only text fields
                                {"prop": "field", "type": "file"}
                            ],
                        },
                    }
                ],
            },
        )
        assert resp.status_code == 200
        assert augment.call_count == 1
        assert augment.call_args.args[0] == kbid
        assert augment.call_args.args[1] == [
            ResourceAugment(
                given=[rid],
                select=[
                    ResourceTitle(),
                    ResourceSummary(),
                ],
            ),
            FieldAugment(given=[rid], select=[FieldText()], filter=[Field(type=FieldTypeName.FILE)]),
        ]
        augment.reset_mock()

        resp = await nucliadb_search.post(
            f"/{KB_PREFIX}/{kbid}/augment",
            json={
                "resources": [
                    {
                        "given": [rid],
                        "fields": {
                            "text": True,
                            "filters": [
                                # try with all other field types
                                {"prop": "field", "type": t}
                                for t in ["text", "link", "generic", "conversation"]
                            ],
                        },
                    }
                ],
            },
        )
        assert resp.status_code == 200
        assert augment.call_count == 1
        assert augment.call_args.args[0] == kbid
        assert augment.call_args.args[1] == [
            FieldAugment(
                given=[rid],
                select=[FieldText()],
                filter=[
                    Field(type=FieldTypeName.TEXT),
                    Field(type=FieldTypeName.LINK),
                    Field(type=FieldTypeName.GENERIC),
                    Field(type=FieldTypeName.CONVERSATION),
                ],
            ),
        ]
        augment.reset_mock()

        #
        # TEST STRATEGY: metadata
        # It augments origin, classification labels, ners and extra metadata
        #
        resp = await nucliadb_search.post(
            f"/{KB_PREFIX}/{kbid}/augment",
            json={
                "resources": [
                    {
                        "given": [rid],
                        "origin": True,
                        "extra": True,
                        "classification_labels": True,
                    }
                ],
                "fields": [
                    {
                        "given": [f"{rid}/f/smb-wonder"],
                        "classification_labels": True,
                        "entities": True,
                    }
                ],
            },
        )
        assert resp.status_code == 200
        assert augment.call_count == 1
        assert augment.call_args.args[0] == kbid
        assert augment.call_args.args[1] == [
            DeepResourceAugment(
                given=[rid],
                show=[ResourceProperties.ORIGIN, ResourceProperties.EXTRA],
                field_type_filter=[
                    FieldTypeName.TEXT,
                    FieldTypeName.FILE,
                    FieldTypeName.LINK,
                    FieldTypeName.CONVERSATION,
                    FieldTypeName.GENERIC,
                    FieldTypeName.KEY_VALUE,
                ],
            ),
            ResourceAugment(given=[rid], select=[ResourceClassificationLabels()]),
            FieldAugment(
                given=[FieldId(rid=rid, type="f", key="smb-wonder")],
                select=[
                    FieldEntities(),
                    FieldClassificationLabels(),
                ],
            ),
        ]
        augment.reset_mock()

        #
        # TEST STRATEGY: neighbour paragraphs
        # It augments the paragraphs surrouding a paragraph
        #
        resp = await nucliadb_search.post(
            f"/{KB_PREFIX}/{kbid}/augment",
            json={
                "paragraphs": [
                    {
                        "given": [
                            {"id": f"{rid}/f/smb-wonder/145-234"},
                        ],
                        "text": True,
                        "neighbours_before": 3,
                        "neighbours_after": 2,
                    }
                ]
            },
        )
        assert resp.status_code == 200
        assert augment.call_count == 1
        assert augment.call_args.args[0] == kbid
        assert augment.call_args.args[1] == [
            ParagraphAugment(
                given=[Paragraph(id=ParagraphId.from_string(f"{rid}/f/smb-wonder/145-234"))],
                select=[
                    ParagraphText(),
                    RelatedParagraphs(neighbours_before=3, neighbours_after=2),
                ],
            )
        ]
        augment.reset_mock()

        #
        # TEST STRATEGY: hiearchy
        # It augments the resource title, summary and paragraph text
        #
        resp = await nucliadb_search.post(
            f"/{KB_PREFIX}/{kbid}/augment",
            json={
                "paragraphs": [
                    {
                        "given": [
                            {"id": f"{rid}/a/title/0-500"},
                            {"id": f"{rid}/a/summary/0-1000"},
                            {"id": f"{rid}/f/smb-wonder/145-234"},
                        ],
                        "text": True,
                    }
                ]
            },
        )
        assert resp.status_code == 200
        assert augment.call_count == 1
        assert augment.call_args.args[0] == kbid
        assert augment.call_args.args[1] == [
            ParagraphAugment(
                given=[
                    Paragraph(id=ParagraphId.from_string(f"{rid}/a/title/0-500")),
                    Paragraph(id=ParagraphId.from_string(f"{rid}/a/summary/0-1000")),
                    Paragraph(id=ParagraphId.from_string(f"{rid}/f/smb-wonder/145-234")),
                ],
                select=[
                    ParagraphText(),
                ],
            )
        ]
        augment.reset_mock()


@pytest.mark.deploy_modes("standalone")
async def test_augment_api_conversation_message_content_text(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    message_payload_1 = {"foo": "bar", "items": [1, 2, 3]}
    message_payload_2 = {"foo": "baz", "items": [4, 5, 6]}
    extracted_payload_1 = {"foo": "bar-extracted", "items": [10, 20, 30]}
    extracted_payload_2 = {"foo": "baz-extracted", "items": [40, 50, 60]}

    create_resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resources",
        json={
            "slug": "json-conversation-resource",
            "title": "JSON Conversation Resource",
            "conversations": {
                "chat": {
                    "messages": [
                        {
                            "to": ["assistant"],
                            "who": "user",
                            "content": {
                                "text": json.dumps(message_payload_1),
                                "format": "JSON",
                            },
                            "ident": "1",
                            "type": "UNSET",
                        },
                        {
                            "to": ["assistant"],
                            "who": "user",
                            "content": {
                                "text": json.dumps(message_payload_2),
                                "format": "JSON",
                            },
                            "ident": "2",
                            "type": "UNSET",
                        },
                    ]
                }
            },
        },
    )
    assert create_resp.status_code == 201
    rid = create_resp.json()["uuid"]

    # Inject extracted text for both messages and ensure augment returns both in one call.
    extracted_split_text = {
        "1": json.dumps(extracted_payload_1),
        "2": json.dumps(extracted_payload_2),
    }

    bm = BrokerMessage()
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.uuid = rid
    bm.kbid = kbid

    field = FieldID(field="chat", field_type=FieldType.CONVERSATION)
    etw = ExtractedTextWrapper()
    etw.field.MergeFrom(field)
    etw.body.text = ""
    etw.body.split_text.update(extracted_split_text)
    bm.extracted_text.append(etw)

    await inject_message(nucliadb_ingest_grpc, bm)

    augment_resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/augment",
        json={
            "fields": [
                {
                    "given": [f"{rid}/c/chat/1", f"{rid}/c/chat/2"],
                    "conversation_message_content_text": True,
                }
            ]
        },
    )
    assert augment_resp.status_code == 200

    field_key = f"{rid}/c/chat"
    body = augment_resp.json()
    assert field_key in body["fields"]

    messages = body["fields"][field_key]["messages"]
    assert messages is not None
    assert len(messages) == 2

    by_ident = {m["ident"]: m for m in messages}
    assert set(by_ident.keys()) == {"1", "2"}
    assert by_ident["1"]["format"] == "JSON"
    assert by_ident["2"]["format"] == "JSON"
    assert json.loads(by_ident["1"]["text"]) == message_payload_1
    assert json.loads(by_ident["2"]["text"]) == message_payload_2

    # Check validation errors for invalid field ids
    for fields_augmentation, error in (
        (
            {"given": [f"{rid}/c/chat/1", f"{rid}/c/chat"], "conversation_message_content_text": True},
            "requires all given field ids to have a subfield_id (aka: ident) in the field id",
        ),
        (
            {
                "given": [f"{rid}/c/chat/1", f"{rid}/c/chat/2"],
                "text": True,
                "conversation_message_content_text": True,
            },
            "`conversation_message_content_text` and `text` are not compatible together",
        ),
    ):
        augment_resp = await nucliadb_search.post(
            f"/{KB_PREFIX}/{kbid}/augment",
            json={"fields": [fields_augmentation]},
        )
        assert augment_resp.status_code == 422
        assert error in augment_resp.json()["detail"][0]["msg"]
