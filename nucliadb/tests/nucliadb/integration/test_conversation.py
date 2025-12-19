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
import random
from datetime import datetime
from typing import Optional
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from nucliadb.reader.api.models import ResourceField
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
from tests.ndbfixtures.resources._vectors import lambs_split_6_vector
from tests.ndbfixtures.resources.lambs import lambs_resource
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
        content="["
        + InputMessage(
            to=[f"computer"],
            content=InputMessageContent(text="42"),
            ident="computer",
            type=MessageType.ANSWER,
        ).model_dump_json(by_alias=True)
        + "]",
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
        query: Optional[str] = None,
        vector: Optional[list[float]] = None,
        top_k: int = 5,
        min_score: Optional[float] = None,
    ) -> KnowledgeboxFindResults:
        payload = {"top_k": top_k, "reranker": "noop"}
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

    async def _test_keyword_search(message_text: str, message_id: str):
        # Test keyword search retrieves the right message
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json={
                "query": message_text,
                "features": ["keyword"],
                "top_k": 1,
            },
        )
        assert resp.status_code == 200
        results = KnowledgeboxFindResults.model_validate(resp.json())
        assert message_id in results.best_matches
        assert (
            results.resources.popitem()[1].fields.popitem()[1].paragraphs.popitem()[1].text
            == message_text
        )

    async def _test_semantic_search(message_text: str, message_id: str, message_vector: list[float]):
        # Test semantic search retrieves the right message
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json={
                "vector": message_vector,
                "features": ["semantic"],
                "top_k": 1,
                "reranker": "noop",
                "vectorset": "multilingual",
            },
        )
        assert resp.status_code == 200
        results = KnowledgeboxFindResults.model_validate(resp.json())
        assert message_id in results.best_matches
        assert (
            results.resources.popitem()[1].fields.popitem()[1].paragraphs.popitem()[1].text
            == message_text
        )

    await _test_keyword_search(message_text, message_id)
    await _test_semantic_search(message_text, message_id, message_vector)

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
    await _test_keyword_search(message_text, message_id)
    await _test_semantic_search(message_text, message_id, message_vector)

    # New message is searchable too
    await _test_keyword_search(new_message_text, new_message_id)
    await _test_semantic_search(new_message_text, new_message_id, new_message_vector)
