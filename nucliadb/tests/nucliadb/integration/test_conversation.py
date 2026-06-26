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
import random
from datetime import datetime
from typing import Any, cast
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from nucliadb.ingest.orm.resource import Resource as ORMResource
from nucliadb.ingest.processing import DummyProcessingEngine
from nucliadb.models.internal.processing import PushMessageFormat, PushPayload
from nucliadb.reader.api.models import ResourceField
from nucliadb.writer.utilities import get_processing
from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
    MessageType,
)
from nucliadb_models.resource import ConversationFieldData, FieldConversation, Resource
from nucliadb_models.resource import Resource as ResponseResponse
from nucliadb_models.search import (
    KnowledgeboxCounters,
    KnowledgeboxFindResults,
    KnowledgeboxSearchResults,
)
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
    Vector,
)
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources._vectors import (
    lambs_split_6_vector,
    lambs_split_7_vector,
    lambs_split_9_vector,
)
from tests.ndbfixtures.resources.lambs import (
    lambs_resource,
    lambs_split_6_text,
    lambs_split_7_text,
    lambs_split_9_text,
)
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import mark_dirty, wait_for_sync


@pytest.fixture(scope="function")
async def resource_with_conversation(
    nucliadb_ingest_grpc, nucliadb_writer: AsyncClient, standalone_knowledgebox
):
    messages = []
    for i in range(1, 301):
        messages.append(
            InputMessage(
                to=["computer"],
                who=f"person{i}",
                timestamp=datetime.now(),
                content=InputMessageContent(text="What is the meaning of life?"),
                ident=str(i),
                type=MessageType.QUESTION,
            )
        )
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        headers={"Content-Type": "application/json"},
        content=CreateResourcePayload(
            slug="myresource",
            conversations={
                "faq": InputConversationField(messages=messages),
            },
        ).model_dump_json(by_alias=True),
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # add another message using the api to add single message
    resp = await nucliadb_writer.put(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/conversation/faq/messages",
        json=[
            InputMessage(
                to=[f"computer"],
                content=InputMessageContent(text="42"),
                ident="computer",
                type=MessageType.ANSWER,
            ).model_dump(by_alias=True)
        ],
    )
    assert resp.status_code == 200

    # Inject synthetic extracted data for the conversation
    extracted_split_text = {"1": "Split text 1", "2": "Split text 2"}

    bm = BrokerMessage()
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.uuid = rid
    bm.kbid = standalone_knowledgebox
    field = FieldID(field="faq", field_type=FieldType.CONVERSATION)

    etw = ExtractedTextWrapper()
    etw.field.MergeFrom(field)
    etw.body.text = ""  # with convos, text is empty
    etw.body.split_text.update(extracted_split_text)
    bm.extracted_text.append(etw)

    fmw = FieldComputedMetadataWrapper()
    fmw.field.MergeFrom(field)
    for split, text in extracted_split_text.items():
        paragraph = Paragraph(start=0, end=len(text), kind=Paragraph.TypeParagraph.TEXT)
        fmw.metadata.split_metadata[split].paragraphs.append(paragraph)
    bm.field_metadata.append(fmw)

    await inject_message(nucliadb_ingest_grpc, bm)

    yield rid


@pytest.mark.deploy_modes("standalone")
async def test_conversations(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
    resource_with_conversation,
):
    rid = resource_with_conversation

    # get field summary
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/resource/{rid}?show=values")
    assert resp.status_code == 200

    res_resp = ResponseResponse.model_validate(resp.json())

    assert res_resp.data.conversations["faq"] == ConversationFieldData(  # type: ignore
        value=FieldConversation(pages=2, size=200, total=301, extract_strategy="", split_strategy=""),
        extracted=None,
        error=None,
    )

    # get first page
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/conversation/faq?page=1"
    )
    assert resp.status_code == 200
    field_resp = ResourceField.model_validate(resp.json())
    assert field_resp.value["total"] == 301  # type: ignore
    assert field_resp.value["pages"] == 2  # type: ignore
    assert field_resp.value["page"] == 1  # type: ignore
    msgs = field_resp.value["messages"]  # type: ignore
    assert len(msgs) == 200
    assert [m["ident"] for m in msgs] == [str(i) for i in range(1, 201)]
    assert msgs[0]["type"] == MessageType.QUESTION.value

    # get second page
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/conversation/faq?page=2"
    )
    assert resp.status_code == 200
    field_resp = ResourceField.model_validate(resp.json())
    msgs = field_resp.value["messages"]  # type: ignore
    assert len(msgs) == 101
    assert [m["ident"] for m in msgs] == [str(i) for i in range(201, 301)] + ["computer"]
    assert msgs[-1]["type"] == MessageType.ANSWER.value


@pytest.mark.deploy_modes("standalone")
async def test_extracted_text_is_serialized_properly(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
    resource_with_conversation,
):
    rid = resource_with_conversation

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=values&show=extracted&extracted=text",
    )
    assert resp.status_code == 200
    resource = Resource.model_validate(resp.json())
    extracted = resource.data.conversations["faq"].extracted  # type: ignore
    assert extracted.text.text == ""  # type: ignore
    assert extracted.text.split_text["1"] == "Split text 1"  # type: ignore
    assert extracted.text.split_text["2"] == "Split text 2"  # type: ignore


@pytest.mark.deploy_modes("standalone")
async def test_find_conversations(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_conversation,
):
    rid = resource_with_conversation

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/find?query=",
    )
    assert resp.status_code == 200
    results = KnowledgeboxFindResults.model_validate(resp.json())
    matching_rid, matching_resource = results.resources.popitem()
    assert matching_rid == rid

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=values&show=extracted&extracted=text"
    )
    assert resp.status_code == 200
    resource = Resource.model_validate(resp.json())

    # Check extracted
    conversation = resource.data.conversations["faq"]  # type: ignore
    extracted = conversation.extracted
    assert extracted.text.text == ""  # type: ignore
    assert extracted.text.split_text["1"] == "Split text 1"  # type: ignore
    assert extracted.text.split_text["2"] == "Split text 2"  # type: ignore

    # Check paragraph positions match the split text values
    field = matching_resource.fields["/c/faq"]
    paragraphs = field.paragraphs
    assert len(paragraphs) == 2
    assert paragraphs[f"{rid}/c/faq/1/0-12"].text == "Split text 1"
    assert paragraphs[f"{rid}/c/faq/2/0-12"].text == "Split text 2"


@pytest.mark.deploy_modes("standalone")
async def test_cannot_create_message_ident_0(
    nucliadb_ingest_grpc, nucliadb_writer: AsyncClient, standalone_knowledgebox: str
):
    messages = [
        # model_construct skips validation, to test the API error
        InputMessage.model_construct(
            to=["computer"],
            who=f"person",
            timestamp=datetime.now(),
            content=InputMessageContent(text="What is the meaning of life?"),
            ident="0",
            type=MessageType.QUESTION,
        )
    ]
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        headers={"Content-Type": "application/json"},
        content=CreateResourcePayload(
            slug="myresource",
            conversations={
                "faq": InputConversationField(messages=messages),
            },
        ).model_dump_json(by_alias=True),
    )

    assert resp.status_code == 422
    assert 'cannot be "0"' in resp.json()["detail"][0]["msg"]


@pytest.mark.deploy_modes("standalone")
async def test_message_idents_are_unique(
    nucliadb_ingest_grpc, nucliadb_writer: AsyncClient, standalone_knowledgebox
):
    message = {
        "timestamp": datetime.now().isoformat(),
        "who": "person",
        "to": ["computer"],
        "content": {"text": "What is the meaning of life?"},
        "ident": "foo_ident",
    }
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "slug": "myresource",
            "conversations": {
                "faq": {
                    "messages": [message, message]  # Two messages with the same ident
                },
            },
        },
    )

    assert resp.status_code == 422
    assert "ident" in resp.text and "is not unique" in resp.text

    # Create a resource first
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Resource",
        },
    )
    assert resp.status_code == 201

    # Add a conversation field with messages with the same ident
    resp = await nucliadb_writer.put(
        f"/kb/{standalone_knowledgebox}/slug/myresource/conversation/faq",
        json={
            "messages": [message, message]  # Two messages with the same ident
        },
    )
    assert resp.status_code == 422
    assert "ident" in resp.text and "is not unique" in resp.text


@pytest.mark.deploy_modes("standalone")
async def test_cannot_create_message_with_slash(
    nucliadb_ingest_grpc, nucliadb_writer: AsyncClient, standalone_knowledgebox: str
):
    messages = [
        # model_construct skips validation, to test the API error
        InputMessage.model_construct(
            to=["computer"],
            who=f"person",
            timestamp=datetime.now(),
            content=InputMessageContent(text="What is the meaning of life?"),
            ident="hello/world",  # This should raise an error
            type=MessageType.QUESTION,
        )
    ]
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        headers={"Content-Type": "application/json"},
        content=CreateResourcePayload(
            slug="myresource",
            conversations={
                "faq": InputConversationField(messages=messages),
            },
        ).model_dump_json(by_alias=True),
    )

    assert resp.status_code == 422
    assert 'cannot contain "/"' in resp.json()["detail"][0]["msg"]


@pytest.mark.deploy_modes("standalone")
async def test_conversation_field_indexing(
    nucliadb_ingest_grpc,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    vectors = {}
    kbid = standalone_knowledgebox

    def _broker_message(split: str, text: str, vector: list[float]):
        bm = BrokerMessage()
        bm.source = BrokerMessage.MessageSource.PROCESSOR
        bm.uuid = rid
        bm.kbid = kbid
        field = FieldID(field="faq", field_type=FieldType.CONVERSATION)

        etw = ExtractedTextWrapper()
        etw.field.MergeFrom(field)
        etw.body.split_text[split] = text
        bm.extracted_text.append(etw)

        fmw = FieldComputedMetadataWrapper()
        fmw.field.MergeFrom(field)
        paragraph = Paragraph(
            start=0,
            end=len(text),
            kind=Paragraph.TypeParagraph.TEXT,
        )
        fmw.metadata.split_metadata[split].paragraphs.append(paragraph)
        bm.field_metadata.append(fmw)

        evw = ExtractedVectorsWrapper()
        evw.field.MergeFrom(field)
        evw.vectors.split_vectors[split].vectors.append(
            Vector(
                start=0,
                end=len(text),
                start_paragraph=0,
                end_paragraph=len(text),
                vector=vector,
            )
        )
        bm.field_vectors.append(evw)
        return bm

    async def search_message(
        query: str | None = None,
        vector: list[float] | None = None,
        top_k: int = 5,
        min_score: float | None = None,
    ) -> KnowledgeboxFindResults:
        payload: dict[str, Any] = {"top_k": top_k, "reranker": "noop"}
        features = []
        if min_score is not None:
            payload["min_score"] = min_score
        if query:
            payload["query"] = query
            features.append("keyword")
        if vector is not None:
            payload["vector"] = vector
            features.append("semantic")
        if not features:
            raise ValueError("At least one of 'query' or 'vector' must be provided")
        payload["features"] = features

        resp = await nucliadb_reader.post(f"/kb/{kbid}/find", json=payload, timeout=None)
        resp.raise_for_status()
        return KnowledgeboxFindResults.model_validate(resp.json())

    async def get_counters() -> KnowledgeboxCounters:
        # We call counters endpoint to get the sentences count only
        resp = await nucliadb_reader.get(f"/kb/{kbid}/counters")
        assert resp.status_code == 200, resp.text
        counters = KnowledgeboxCounters.model_validate(resp.json())
        n_sentences = counters.sentences

        # We don't call /counters endpoint purposefully, as deletions are not guaranteed to be merged yet.
        # Instead, we do some searches.
        resp = await nucliadb_reader.get(f"/kb/{kbid}/search", params={"show": ["basic", "values"]})
        assert resp.status_code == 200, resp.text
        search = KnowledgeboxSearchResults.model_validate(resp.json())
        n_resources = len(search.resources)
        n_paragraphs = search.paragraphs.total  # type: ignore
        n_fields = sum(
            [
                len(resource.data.generics or {})
                + len(resource.data.files or {})
                + len(resource.data.links or {})
                + len(resource.data.texts or {})
                + len(resource.data.conversations or {})
                for resource in search.resources.values()
                if resource.data
            ]
        )
        # Update the counters object
        counters.resources = n_resources
        counters.paragraphs = n_paragraphs
        counters.sentences = n_sentences
        counters.fields = n_fields
        return counters

    # Make sure counters are empty
    counters = await get_counters()
    assert counters.sentences == 0
    assert counters.paragraphs == 0
    assert counters.fields == 0
    assert counters.resources == 0

    # Add conversation field with messages
    question = "What is the meaning of life?"
    vectors[question] = [1.0] * 768

    slug = "myresource"
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": slug,
            "title": "My Resource",
            "conversations": {
                "faq": {
                    "messages": [
                        {
                            "to": ["computer"],
                            "who": "person",
                            "timestamp": datetime.now().isoformat(),
                            "content": {"text": question},
                            "ident": "1",
                            "type": MessageType.QUESTION.value,
                        },
                    ]
                },
            },
        },
    )
    resp.raise_for_status()
    rid = resp.json()["uuid"]

    # Inject synthetic processed data for the conversation
    await inject_message(
        nucliadb_ingest_grpc, _broker_message(split="1", text=question, vector=vectors[question])
    )
    await mark_dirty()
    await wait_for_sync()

    # Check counters
    counters = await get_counters()
    assert counters.sentences == 1  # One for the question message
    assert counters.paragraphs == 2  # One for the question message + title paragraph
    assert counters.fields == 2  # conversation field + title field
    assert counters.resources == 1

    # Append a message
    answer = "42"
    vectors[answer] = [2.0] * 768
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/resource/{rid}/conversation/faq/messages",
        json=[
            {
                "to": ["person"],
                "who": "computer",
                "timestamp": datetime.now().isoformat(),
                "content": {"text": answer},
                "ident": "2",
                "type": MessageType.ANSWER.value,
            }
        ],
    )
    resp.raise_for_status()

    await inject_message(
        nucliadb_ingest_grpc, _broker_message(split="2", text=answer, vector=vectors[answer])
    )

    await mark_dirty()
    await wait_for_sync()

    # Check counters after appending the message
    counters = await get_counters()
    assert counters.sentences == 2  # One for each message (the title does not have a vector)
    assert counters.paragraphs == 3  # One for each message + the title paragraph
    assert counters.fields == 2  # One conversation field + the title field
    assert counters.resources == 1

    # Make sure the messages are searchable
    question_text_block_id = f"{rid}/c/faq/1/0-{len(question)}"
    results = await search_message(query=question)
    assert len(results.best_matches) == 1
    assert question_text_block_id in results.best_matches

    # TODO: Look into why both messages of the conversation get the same vector score!
    results = await search_message(vector=vectors[question], top_k=3, min_score=-1)
    assert len(results.best_matches) == 2
    assert question_text_block_id in results.best_matches

    # Remove the field
    resp = await nucliadb_writer.delete(f"/kb/{kbid}/resource/{rid}/conversation/faq")
    resp.raise_for_status()

    await mark_dirty()
    await wait_for_sync()

    # Make sure the messages are not searchable anymore
    counters = await get_counters()
    assert (
        counters.sentences == 2
    )  # The messages are not indexed anymore, but deleted messages still count
    assert counters.paragraphs == 1  # the title
    assert counters.fields == 1  # the title
    assert counters.resources == 1

    results = await search_message(query=question)
    assert len(results.best_matches) == 0
    results = await search_message(vector=vectors[question], top_k=3, min_score=-1)
    assert len(results.best_matches) == 0


@pytest.mark.deploy_modes("standalone")
async def test_conversation_field_empty_create(
    nucliadb_ingest_grpc,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    # Create an conversation field with an empty conversation first
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        headers={"Content-Type": "application/json"},
        content=CreateResourcePayload(
            slug="myresource",
            conversations={
                "faq": InputConversationField(),
            },
        ).model_dump_json(by_alias=True),
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Now append a message
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/resource/{rid}/conversation/faq/messages",
        json=[
            {
                "to": ["computer"],
                "who": "person1",
                "timestamp": datetime.now().isoformat(),
                "content": {"text": "What is the meaning of life?"},
                "ident": "1",
                "type": MessageType.QUESTION.value,
            }
        ],
    )
    resp.raise_for_status()

    # Get the conversation field and check that the message is in page 1
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}/conversation/faq",
        params={
            "page": 1,
            "show": ["value"],
        },
    )
    assert resp.status_code == 200
    field_resp = ResourceField.model_validate(resp.json())
    msgs = field_resp.value["messages"]  # type: ignore
    assert len(msgs) == 1
    assert msgs[0]["ident"] == "1"


@pytest.mark.deploy_modes("standalone")
async def test_conversation_limits(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    slug = "myresource"

    with patch("nucliadb.writer.resource.field.MAX_CONVERSATION_MESSAGES", 2):
        # Create a conversation field
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "slug": slug,
                "title": "My Resource",
                "conversations": {
                    "faq": {
                        "messages": [
                            {
                                "to": ["computer"],
                                "who": "person",
                                "timestamp": datetime.now().isoformat(),
                                "content": {"text": "foo"},
                                "ident": "1",
                                "type": MessageType.QUESTION.value,
                            }
                        ]
                    },
                },
            },
        )
        resp.raise_for_status()

        # Now append a couple of messages so it exceeds the maximum allowed
        resp = await nucliadb_writer.put(
            f"/kb/{kbid}/slug/{slug}/conversation/faq/messages",
            json=[
                {
                    "to": ["computer"],
                    "who": "person",
                    "timestamp": datetime.now().isoformat(),
                    "content": {"text": "bar"},
                    "ident": "2",
                    "type": MessageType.QUESTION.value,
                },
                {
                    "to": ["person"],
                    "who": "computer",
                    "timestamp": datetime.now().isoformat(),
                    "content": {"text": "baz"},
                    "ident": "3",
                    "type": MessageType.ANSWER.value,
                },
            ],
        )
        assert resp.status_code == 422
        assert resp.json()["detail"] == "Conversation fields cannot have more than 2 messages."


@pytest.mark.deploy_modes("standalone")
async def test_conversation_duplicate_message_idents(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    slug = "myresource"

    # Create a conversation field
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": slug,
            "title": "My Resource",
            "conversations": {
                "faq": {
                    "messages": [
                        {
                            "to": ["computer"],
                            "who": "person",
                            "timestamp": datetime.now().isoformat(),
                            "content": {"text": "foo"},
                            "ident": "1",
                            "type": MessageType.QUESTION.value,
                        }
                    ]
                },
            },
        },
    )
    resp.raise_for_status()

    # Now append a message with a duplicate ident
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/slug/{slug}/conversation/faq/messages",
        json=[
            {
                "to": ["computer"],
                "who": "person",
                "timestamp": datetime.now().isoformat(),
                "content": {"text": "bar"},
                "ident": "1",
                "type": MessageType.QUESTION.value,
            },
        ],
    )
    assert resp.status_code == 422
    assert resp.json()["detail"] == "Message identifiers must be unique field=faq: ['1']"


@pytest.mark.deploy_modes("standalone")
async def test_replace_conversation_with_put_endpoint_deletes_previous_pages(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    slug = "myresource"

    # Create a conversation field
    message1 = {
        "to": ["computer"],
        "who": "person",
        "timestamp": datetime.now().isoformat(),
        "content": {"text": "foo"},
        "ident": "1",
        "type": MessageType.QUESTION.value,
    }
    message2 = {
        "to": ["person"],
        "who": "computer",
        "timestamp": datetime.now().isoformat(),
        "content": {"text": "bar"},
        "ident": "2",
        "type": MessageType.ANSWER.value,
    }

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": slug,
            "title": "My Resource",
            "conversations": {"faq": {"messages": [message1, message2]}},
        },
    )
    resp.raise_for_status()

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/slug/{slug}/conversation/faq", params={"show": ["value"]}
    )
    resp.raise_for_status()
    body = resp.json()
    assert len(body["value"]["messages"]) == 2
    assert body["value"]["messages"][0]["ident"] == "1"
    assert body["value"]["messages"][1]["ident"] == "2"

    # Now replace the conversation with a put
    message3 = {
        "to": ["person"],
        "who": "computer",
        "timestamp": datetime.now().isoformat(),
        "content": {"text": "foo"},
        "ident": "x",
        "type": MessageType.ANSWER.value,
    }
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/slug/{slug}/conversation/faq",
        json={"messages": [message3]},
    )
    assert resp.status_code == 201

    # Make sure that the previous value has been deleted
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/slug/{slug}/conversation/faq", params={"show": ["value"]}
    )
    resp.raise_for_status()
    body = resp.json()
    assert len(body["value"]["messages"]) == 1
    assert body["value"]["messages"][0]["ident"] == "x"


async def _test_keyword_search(
    nucliadb_reader: AsyncClient,
    kbid: str,
    message_text: str,
    message_id: str,
    top_k: int = 1,
    min_score: float | dict | None = None,
    found: bool = True,
):
    payload: dict[str, Any] = {
        "query": message_text,
        "features": ["keyword"],
        "top_k": top_k,
        "reranker": "noop",
    }
    if min_score is not None:
        payload["min_score"] = min_score

    # Test keyword search retrieves the right message
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json=payload,
    )
    assert resp.status_code == 200
    results = KnowledgeboxFindResults.model_validate(resp.json())

    if found:
        assert message_id in results.best_matches
        assert (
            results.resources.popitem()[1].fields.popitem()[1].paragraphs.popitem()[1].text
            == message_text
        )
    else:
        assert message_id not in results.best_matches


async def _test_semantic_search(
    nucliadb_reader: AsyncClient,
    kbid: str,
    message_text: str,
    message_id: str,
    message_vector: list[float],
    top_k: int = 1,
    min_score: float | dict | None = None,
    found: bool = True,
):
    payload: dict[str, Any] = {
        "vector": message_vector,
        "features": ["semantic"],
        "top_k": top_k,
        "reranker": "noop",
        "vectorset": "multilingual",
    }
    if min_score is not None:
        payload["min_score"] = min_score
    # Test semantic search retrieves the right message
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json=payload,
    )
    assert resp.status_code == 200
    results = KnowledgeboxFindResults.model_validate(resp.json())
    if found:
        assert message_id in results.best_matches
        assert (
            results.resources.popitem()[1].fields.popitem()[1].paragraphs.popitem()[1].text
            == message_text
        )
    else:
        assert message_id not in results.best_matches


@pytest.mark.deploy_modes("standalone")
async def test_conversation_search(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    rid = await lambs_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    message_text = "Orion is looking splendid tonight, and Arcturus, the Herdsman, with his flock..."
    message_id = f"{rid}/c/lambs/6/0-80"
    message_vector = lambs_split_6_vector[:512]

    await _test_keyword_search(nucliadb_reader, kbid, message_text, message_id)
    await _test_semantic_search(nucliadb_reader, kbid, message_text, message_id, message_vector)

    # Add another message to the conversation, to test that indexing works on partial updates too
    new_message_text = "Foo barba foo barba foo."
    new_message_vector = [random.uniform(-1, 1) for _ in range(512)]
    new_message_split_id = "new_message"
    new_message_id = f"{rid}/c/lambs/{new_message_split_id}/0-{len(new_message_text)}"

    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/resource/{rid}/conversation/lambs/messages",
        json=[
            {
                "to": ["reader"],
                "who": "narrator",
                "timestamp": datetime.now().isoformat(),
                "content": {"text": new_message_text},
                "ident": new_message_split_id,
                "type": MessageType.UNSET.value,
            }
        ],
    )
    resp.raise_for_status()

    # Inject synthetic processed data for the new message
    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    conv = bmb.field_builder("lambs", FieldType.CONVERSATION)
    conv.add_paragraph(
        text=new_message_text,
        split=new_message_split_id,
        vectors={"multilingual": new_message_vector},
    )
    bm = bmb.build()
    await inject_message(nucliadb_ingest_grpc, bm)
    await mark_dirty()
    await wait_for_sync()

    # Previous searches still work
    await _test_keyword_search(nucliadb_reader, kbid, message_text, message_id)
    await _test_semantic_search(nucliadb_reader, kbid, message_text, message_id, message_vector)

    # New message is searchable too
    await _test_keyword_search(nucliadb_reader, kbid, new_message_text, new_message_id)
    await _test_semantic_search(
        nucliadb_reader, kbid, new_message_text, new_message_id, new_message_vector
    )


@pytest.mark.deploy_modes("standalone")
async def test_append_messages_to_non_existent_conversation_field_creates_it(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    """Test that appending messages to a non-existent conversation field creates it."""
    kbid = standalone_knowledgebox

    # Create a resource without any conversation field
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": "test_resource",
            "title": "Test Resource",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Try to get the conversation field before appending - should fail
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}/conversation/new_conversation",
        params={"page": 1},
    )
    assert resp.status_code == 404

    # Append messages to a non-existent conversation field
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/resource/{rid}/conversation/new_conversation/messages",
        json=[
            {
                "to": ["assistant"],
                "who": "user",
                "timestamp": datetime.now().isoformat(),
                "content": {"text": "Hello, how are you?"},
                "ident": "1",
                "type": MessageType.QUESTION.value,
            },
            {
                "to": ["user"],
                "who": "assistant",
                "timestamp": datetime.now().isoformat(),
                "content": {"text": "I'm doing well, thank you!"},
                "ident": "2",
                "type": MessageType.ANSWER.value,
            },
        ],
    )
    assert resp.status_code == 200
    assert "seqid" in resp.json()

    # Get the conversation field and verify the messages were added
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}/conversation/new_conversation",
        params={"page": 1, "show": ["value"]},
    )
    assert resp.status_code == 200
    field_resp = ResourceField.model_validate(resp.json())
    msgs = field_resp.value["messages"]  # type: ignore
    assert len(msgs) == 2
    assert msgs[0]["ident"] == "1"
    assert msgs[0]["who"] == "user"
    assert msgs[0]["content"]["text"] == "Hello, how are you?"
    assert msgs[1]["ident"] == "2"
    assert msgs[1]["who"] == "assistant"
    assert msgs[1]["content"]["text"] == "I'm doing well, thank you!"

    # Verify the field exists in the resource summary
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}",
        params={"show": ["values"]},
    )
    assert resp.status_code == 200
    res_resp = ResponseResponse.model_validate(resp.json())
    assert "new_conversation" in res_resp.data.conversations  # type: ignore
    conv_field = res_resp.data.conversations["new_conversation"]  # type: ignore
    assert conv_field.value is not None
    assert conv_field.value.total == 2
    assert conv_field.value.pages == 1


@pytest.mark.deploy_modes("standalone")
async def test_delete_conversation_message_lambs_resource(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    # Create a resource with the lambs conversation
    rid = await lambs_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    lambs_split_6_vector_m = lambs_split_6_vector[:512]
    lambs_split_7_vector_m = lambs_split_7_vector[:512]
    lambs_split_9_vector_m = lambs_split_9_vector[:512]

    lambs_split_6_pid = f"{rid}/c/lambs/6/0-{len(lambs_split_6_text)}"
    lambs_split_7_pid = f"{rid}/c/lambs/7/0-{len(lambs_split_7_text)}"
    lambs_split_9_pid = f"{rid}/c/lambs/9/0-{len(lambs_split_9_text)}"

    # Verify all 3 messages are present
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}/conversation/lambs",
        params={"page": 1, "show": ["value"]},
    )
    assert resp.status_code == 200
    field_resp = ResourceField.model_validate(resp.json())
    msgs = field_resp.value["messages"]  # type: ignore
    assert len(msgs) > 0
    assert {"6", "7", "9"}.issubset({m["ident"] for m in msgs})

    # Verify all messages are searchable before deletion
    search_cases = {
        "split_6": {
            "text": lambs_split_6_text,
            "vector": lambs_split_6_vector_m,
            "pid": lambs_split_6_pid,
            "should_find": True,
        },
        "split_7": {
            "text": lambs_split_7_text,
            "vector": lambs_split_7_vector_m,
            "pid": lambs_split_7_pid,
            "should_find": True,
        },
        "split_9": {
            "text": lambs_split_9_text,
            "vector": lambs_split_9_vector_m,
            "pid": lambs_split_9_pid,
            "should_find": True,
        },
    }

    async def _check_search():
        for case in search_cases.values():
            await _test_keyword_search(
                nucliadb_reader,
                kbid,
                cast(str, case["text"]),
                cast(str, case["pid"]),
                found=cast(bool, case["should_find"]),
            )
            await _test_semantic_search(
                nucliadb_reader,
                kbid,
                cast(str, case["text"]),
                cast(str, case["pid"]),
                cast(list[int | float], case["vector"]),
                found=cast(bool, case["should_find"]),
            )

    await _check_search()

    # Delete the second message by resource id
    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/resource/{rid}/conversation/lambs/messages/7",
    )
    assert resp.status_code == 204

    await mark_dirty()
    await wait_for_sync()

    # Verify 7 is gone but 6 and 9 remain
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}/conversation/lambs",
        params={"page": 1, "show": ["value"]},
    )
    assert resp.status_code == 200
    field_resp = ResourceField.model_validate(resp.json())
    msgs = {m["ident"]: m for m in field_resp.value["messages"]}  # type: ignore
    assert {"6", "9"}.issubset(msgs.keys())
    assert "7" not in msgs

    # Verify split 7 is no longer found by keyword search, but splits 6 and 9 are still searchable
    search_cases["split_7"]["should_find"] = False
    await _check_search()

    # Delete the second message again -- should be a no-op (already deleted)
    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/resource/{rid}/conversation/lambs/messages/7",
    )
    assert resp.status_code == 204

    # Delete a non-existent message -- should be a no-op
    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/resource/{rid}/conversation/lambs/messages/nonexistent",
    )
    assert resp.status_code == 204

    # Delete using slug endpoint
    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/slug/lambs/conversation/lambs/messages/9",
    )
    assert resp.status_code == 204

    await mark_dirty()
    await wait_for_sync()

    # Verify only split 6 remains
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}/conversation/lambs",
        params={"page": 1, "show": ["value"]},
    )
    assert resp.status_code == 200
    field_resp = ResourceField.model_validate(resp.json())
    msgs = field_resp.value["messages"]  # type: ignore
    assert {"6"}.issubset({m["ident"] for m in msgs})

    # Verify split 6 is still searchable
    search_cases["split_9"]["should_find"] = False
    await _check_search()

    # Verify error cases

    # Non-existent resource
    nonexistent_rid = ORMResource.new_unique_rid()
    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/resource/{nonexistent_rid}/conversation/lambs/messages/6",
    )
    assert resp.status_code == 404

    # Non-existent field
    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/resource/{rid}/conversation/nonexistent_field/messages/6",
    )
    assert resp.status_code == 404

    # Message ident too long (> 128 chars)
    long_ident = "a" * 129
    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/resource/{rid}/conversation/lambs/messages/{long_ident}",
    )
    assert resp.status_code == 422

    # Verify that a deleted message ident cannot be reused
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/resource/{rid}/conversation/lambs/messages",
        json=[
            {
                "to": ["assistant"],
                "who": "user",
                "timestamp": datetime.now().isoformat(),
                "content": {"text": "Trying to reuse deleted ident"},
                "ident": "7",  # This was deleted earlier
                "type": MessageType.QUESTION.value,
            },
        ],
    )
    assert resp.status_code == 422
    assert "must be unique" in resp.json()["detail"]


@pytest.mark.deploy_modes("standalone")
async def test_reprocess_conversation_with_json_message(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    processing = get_processing()
    assert isinstance(processing, DummyProcessingEngine)

    # Create a resource with a conversation field with one message which has JSON content
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": "test_resource",
            "title": "Test Resource",
            "conversations": {
                "chat": {
                    "messages": [
                        {
                            "to": ["assistant"],
                            "who": "user",
                            "timestamp": datetime.now().isoformat(),
                            "content": {"text": json.dumps({"foo": "bar"}), "format": "JSON"},
                            "ident": "1",
                            "type": MessageType.UNSET.value,
                        }
                    ]
                },
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    processing.calls.clear()
    processing.values.clear()

    # Call the reprocess endpoint for the resource, make sure it doesn't fail.
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resource/{rid}/reprocess",
    )
    assert resp.status_code == 202

    # Validate send to process payload
    assert len(processing.values["send_to_process"]) == 1
    send_to_process_call = cast(PushPayload, processing.values["send_to_process"][0][0])
    push_conv = send_to_process_call.conversationfield["chat"]
    assert push_conv.messages[0].content.format == PushMessageFormat.JSON


@pytest.mark.deploy_modes("standalone")
async def test_append_messages_sends_generated_conversation_to_processing(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    source_conv_id = "chat"
    generated_conv_id = f"da-facts-c-{source_conv_id}"

    # Create a new conversation
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": "test_resource",
            "title": "Test Resource",
            "conversations": {
                source_conv_id: {
                    "messages": [
                        {
                            "to": ["assistant"],
                            "who": "user",
                            "timestamp": datetime.now().isoformat(),
                            "content": {
                                "text": "Things were simpler in the past. Did you know that the first computers were the size of a room?"
                                "They were used for complex calculations and were incredibly expensive. It's fascinating to see how"
                                "far technology has come since then.",
                            },
                            "ident": "1",
                            "type": MessageType.UNSET.value,
                        }
                    ],
                },
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Create its fake generated conversation, as if it was generated by a data augmentation
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/resource/{rid}/conversation/{generated_conv_id}/messages",
        json=[
            {
                "to": ["user"],
                "who": "assistant",
                "timestamp": datetime.now().isoformat(),
                "content": {
                    "text": "It's interesting to think about how far we've come in such a short time.",
                },
                "ident": "1",
                "type": MessageType.UNSET.value,
            },
            {
                "to": ["assistant"],
                "who": "user",
                "timestamp": datetime.now().isoformat(),
                "content": {
                    "text": "Indeed! The evolution of technology has been rapid and transformative, shaping the way we live and work in profound ways.",
                },
                "ident": "2",
                "type": MessageType.UNSET.value,
            },
        ],
    )
    assert resp.status_code == 200

    # Append a message to the source conversation, and check that the generated conversation is sent to processing with the new message
    processing = get_processing()
    assert isinstance(processing, DummyProcessingEngine)
    processing.calls.clear()
    processing.values.clear()

    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/resource/{rid}/conversation/{source_conv_id}/messages",
        json=[
            {
                "to": ["assistant"],
                "who": "user",
                "timestamp": datetime.now().isoformat(),
                "content": {
                    "text": "The first computer programmer was Ada Lovelace, who lived in the 19th century.",
                },
                "ident": "2",
                "type": MessageType.UNSET.value,
            }
        ],
    )
    assert resp.status_code == 200

    # Make sure that the push payload includes the newly added message and the generated conversation is sent to processing
    assert len(processing.values["send_to_process"]) == 1
    push_payload = cast(PushPayload, processing.values["send_to_process"][0][0])

    assert source_conv_id in push_payload.conversationfield
    assert len(push_payload.conversationfield[source_conv_id].messages) == 1
    assert (
        push_payload.conversationfield[source_conv_id].messages[0].content.text
        == "The first computer programmer was Ada Lovelace, who lived in the 19th century."
    )

    # Check that the full generated conversation is sent
    assert generated_conv_id in push_payload.generated_conversationfield
    assert push_payload.generated_conversationfield[generated_conv_id].source_field_id == source_conv_id
    assert (
        len(push_payload.generated_conversationfield[generated_conv_id].conversationfield.messages) == 2
    )
    assert (
        push_payload.generated_conversationfield[generated_conv_id]
        .conversationfield.messages[0]
        .content.text
        == "It's interesting to think about how far we've come in such a short time."
    )
    assert (
        push_payload.generated_conversationfield[generated_conv_id]
        .conversationfield.messages[1]
        .content.text
        == "Indeed! The evolution of technology has been rapid and transformative, shaping the way we live and work in profound ways."
    )
