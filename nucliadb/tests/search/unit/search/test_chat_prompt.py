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
import base64
from unittest import mock
from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB, ParagraphId
from nucliadb.search.search.chat import prompt as chat_prompt
from nucliadb.search.search.metrics import Metrics
from nucliadb_models.search import (
    SCORE_TYPE,
    FindField,
    FindParagraph,
    FindResource,
    HierarchyResourceStrategy,
    Image,
    KnowledgeboxFindResults,
    MetadataExtensionStrategy,
    MetadataExtensionType,
    MinScore,
    PageImageStrategy,
    ParagraphImageStrategy,
    TableImageStrategy,
)
from nucliadb_protos import resources_pb2 as rpb2


@pytest.fixture()
def messages():
    msgs = [
        rpb2.Message(ident="1", content=rpb2.MessageContent(text="Message 1")),
        rpb2.Message(ident="2", content=rpb2.MessageContent(text="Message 2")),
        rpb2.Message(
            ident="3",
            who="1",
            content=rpb2.MessageContent(text="Message 3"),
            type=rpb2.Message.MessageType.QUESTION,
        ),
        rpb2.Message(
            ident="4",
            content=rpb2.MessageContent(text="Message 4"),
            type=rpb2.Message.MessageType.ANSWER,
            to=["1"],
        ),
        rpb2.Message(ident="5", content=rpb2.MessageContent(text="Message 5")),
    ]
    yield msgs


@pytest.fixture()
def field_obj(messages):
    mock = AsyncMock()
    mock.get_metadata.return_value = rpb2.FieldConversation(pages=1, total=5)
    mock.db_get_value.return_value = rpb2.Conversation(messages=messages)

    yield mock


@pytest.fixture()
def kb(field_obj):
    mock = AsyncMock()
    mock.get.return_value.get_field.return_value = field_obj
    yield mock


async def test_get_next_conversation_messages(field_obj, messages):
    assert (
        len(
            await chat_prompt.get_next_conversation_messages(
                field_obj=field_obj, page=1, start_idx=0, num_messages=5
            )
        )
        == 5
    )
    assert (
        len(
            await chat_prompt.get_next_conversation_messages(
                field_obj=field_obj, page=1, start_idx=0, num_messages=1
            )
        )
        == 1
    )

    assert await chat_prompt.get_next_conversation_messages(
        field_obj=field_obj,
        page=1,
        start_idx=0,
        num_messages=1,
        message_type=rpb2.Message.MessageType.ANSWER,
        msg_to="1",
    ) == [messages[3]]


async def test_find_conversation_message(field_obj, messages):
    assert await chat_prompt.find_conversation_message(field_obj=field_obj, mident="3") == (
        messages[2],
        1,
        2,
    )


async def test_get_expanded_conversation_messages(kb, messages):
    assert await chat_prompt.get_expanded_conversation_messages(
        kb=kb, rid="rid", field_id="field_id", mident="3"
    ) == [messages[3]]


async def test_get_expanded_conversation_messages_question(kb, messages):
    assert (
        await chat_prompt.get_expanded_conversation_messages(
            kb=kb, rid="rid", field_id="field_id", mident="1"
        )
        == messages[1:]
    )

    kb.get.assert_called_with("rid")
    kb.get.return_value.get_field.assert_called_with("field_id", FIELD_TYPE_STR_TO_PB["c"], load=True)


async def test_get_expanded_conversation_messages_missing(kb, messages):
    assert (
        await chat_prompt.get_expanded_conversation_messages(
            kb=kb, rid="rid", field_id="field_id", mident="missing"
        )
        == []
    )


def get_ordered_paragraphs(find_results):
    paragraphs = []
    for resource in find_results.resources.values():
        for field in resource.fields.values():
            paragraphs.extend(field.paragraphs.values())
    paragraphs.sort(key=lambda p: p.order)
    return paragraphs


def _create_find_result(
    paragraph: FindParagraph,
):
    pid = ParagraphId.from_string(paragraph.id)
    rid = pid.rid
    fid = f"{pid.field_id.type}/{pid.field_id.key}"
    return FindResource(
        id=rid,
        fields={
            fid: FindField(
                paragraphs={
                    pid.full(): paragraph,
                }
            )
        },
    )


async def test_default_prompt_context(kb):
    result_text = " ".join(["text"] * 10)
    with (
        patch("nucliadb.search.search.chat.prompt.get_driver"),
        patch("nucliadb.search.search.chat.prompt.get_storage"),
        patch("nucliadb.search.search.chat.prompt.KnowledgeBoxORM", return_value=kb),
    ):
        context = chat_prompt.CappedPromptContext(max_size=int(1e6))
        find_results = KnowledgeboxFindResults(
            facets={},
            resources={
                "bmid": _create_find_result(
                    FindParagraph(
                        id="bmid/c/conv/ident/0-1",
                        score=1,
                        score_type=SCORE_TYPE.BM25,
                        order=1,
                        text=result_text,
                    )
                ),
                "vecid": _create_find_result(
                    FindParagraph(
                        id="vecid/c/conv/ident/0-1",
                        score=0,
                        score_type=SCORE_TYPE.VECTOR,
                        order=2,
                        text=result_text,
                    )
                ),
                "both_id": _create_find_result(
                    FindParagraph(
                        id="both_id/c/conv/ident/0-1",
                        score=2,
                        score_type=SCORE_TYPE.BOTH,
                        order=0,
                        text=result_text,
                    )
                ),
            },
        )
        await chat_prompt.default_prompt_context(
            context,
            "kbid",
            get_ordered_paragraphs(find_results),
        )
        prompt_result = context.output
        # Check that the results are sorted by increasing order and that the extra
        # context is added at the beginning, indicating that it has the most priority
        paragraph_ids = [pid for pid in prompt_result.keys()]
        assert paragraph_ids == [
            "both_id/c/conv/ident/0-1",
            "bmid/c/conv/ident/0-1",
            "vecid/c/conv/ident/0-1",
        ]


@pytest.fixture(scope="function")
def find_results():
    return KnowledgeboxFindResults(
        facets={},
        resources={
            "resource1": _create_find_result(
                FindParagraph(
                    id="resource1/a/title/0-10",
                    score=1,
                    score_type=SCORE_TYPE.BOTH,
                    order=1,
                    text="Resource 1",
                )
            ),
            "resource2": _create_find_result(
                FindParagraph(
                    id="resource2/a/title/0-10",
                    score=2,
                    score_type=SCORE_TYPE.VECTOR,
                    order=2,
                    text="Resource 2",
                )
            ),
        },
        min_score=MinScore(semantic=-1),
    )


async def test_prompt_context_builder_prepends_user_context(
    find_results: KnowledgeboxFindResults,
):
    builder = chat_prompt.PromptContextBuilder(
        kbid="kbid",
        ordered_paragraphs=get_ordered_paragraphs(find_results),
        user_context=["Carrots are orange"],
    )

    async def _mock_build_context(context, *args, **kwargs):
        context["resource1/a/title"] = "Resource 1"
        context["resource2/a/title"] = "Resource 2"

    with mock.patch.object(builder, "_build_context", new=_mock_build_context):
        context, context_order, image_context, augmented_context = await builder.build()
        assert len(context) == 3
        assert len(context_order) == 3
        assert len(image_context) == 0
        assert context["USER_CONTEXT_0"] == "Carrots are orange"
        assert context["resource1/a/title"] == "Resource 1"
        assert context["resource2/a/title"] == "Resource 2"
        assert context_order["USER_CONTEXT_0"] == 0
        assert context_order["resource1/a/title"] == 1
        assert context_order["resource2/a/title"] == 2


def test_capped_prompt_context():
    context = chat_prompt.CappedPromptContext(max_size=2)

    # Check that output is trimmed
    context["key1"] = "123"

    assert context.output == {"key1": "12"}
    assert context.size == 2

    # Update existing value
    context["key1"] = "foobar"
    assert context.output == {"key1": "fo"}
    assert context.size == 2

    # Check text block ids
    assert context.text_block_ids() == ["key1"]

    # Check without limits
    context = chat_prompt.CappedPromptContext(max_size=None)
    context["key1"] = "foo" * int(1e6)

    assert context.output == {"key1": "foo" * int(1e6)}
    assert context.size == int(3e6)

    # Check that the size is updated correctly upon deletion
    del context["key1"]
    assert context.size == 0

    # Deletion of non-existing key should not raise an error
    del context["key1337"]
    assert context.size == 0


async def test_hierarchy_promp_context(kb):
    with mock.patch(
        "nucliadb.search.search.chat.prompt.get_paragraph_text",
        side_effect=["Title text", "Summary text"],
    ):
        context = chat_prompt.CappedPromptContext(max_size=int(1e6))
        find_results = KnowledgeboxFindResults(
            resources={
                "r1": FindResource(
                    id="r1",
                    fields={
                        "f/f1": FindField(
                            paragraphs={
                                "r1/f/f1/0-10": FindParagraph(
                                    id="r1/f/f1/0-10",
                                    score=10,
                                    score_type=SCORE_TYPE.BM25,
                                    order=0,
                                    text="First Paragraph text",
                                ),
                                "r1/f/f1/10-20": FindParagraph(
                                    id="r1/f/f1/10-20",
                                    score=8,
                                    score_type=SCORE_TYPE.BM25,
                                    order=1,
                                    text="Second paragraph text",
                                ),
                            }
                        )
                    },
                )
            },
        )
        ordered_paragraphs = get_ordered_paragraphs(find_results)
        await chat_prompt.hierarchy_prompt_context(
            context, "kbid", ordered_paragraphs, HierarchyResourceStrategy(), Metrics("foo")
        )
        assert (
            context.output["r1/f/f1/0-10"]
            == "DOCUMENT: Title text \n SUMMARY: Summary text \n RESOURCE CONTENT: \n EXTRACTED BLOCK: \n First Paragraph text \n\n \n EXTRACTED BLOCK: \n Second paragraph text"  # noqa
        )
        # Chec that the original text of the paragraphs is preserved
        assert ordered_paragraphs[0].text == "First Paragraph text"
        assert ordered_paragraphs[1].text == "Second paragraph text"


def test_get_neighbouring_paragraph_indexes():
    field_paragraphs = [
        ParagraphId.from_string("r1/f/f1/0-10"),
        ParagraphId.from_string("r1/f/f1/10-20"),
        ParagraphId.from_string("r1/f/f1/20-30"),
        ParagraphId.from_string("r1/f/f1/30-40"),
        ParagraphId.from_string("r1/f/f1/40-50"),
        ParagraphId.from_string("r1/f/f1/50-60"),
        ParagraphId.from_string("r1/f/f1/60-70"),
        ParagraphId.from_string("r1/f/f1/70-80"),
    ]
    matching_paragraph = field_paragraphs[2]

    assert chat_prompt.get_neighbouring_paragraph_indexes(
        field_paragraphs, matching_paragraph, before=100, after=100
    ) == [0, 1, 2, 3, 4, 5, 6, 7]

    assert chat_prompt.get_neighbouring_paragraph_indexes(
        field_paragraphs, matching_paragraph, before=2, after=2
    ) == [0, 1, 2, 3, 4]

    assert chat_prompt.get_neighbouring_paragraph_indexes(
        field_paragraphs, matching_paragraph, before=1, after=0
    ) == [1, 2]

    assert chat_prompt.get_neighbouring_paragraph_indexes(
        field_paragraphs, matching_paragraph, before=0, after=1
    ) == [2, 3]

    assert chat_prompt.get_neighbouring_paragraph_indexes(
        field_paragraphs, matching_paragraph, before=0, after=0
    ) == [2]


async def test_extend_prompt_context_with_metadata():
    origin = rpb2.Origin()
    origin.tags.extend(["tag1", "tag2"])
    origin.metadata.update({"foo": "bar"})
    basic = rpb2.Basic()
    basic.usermetadata.classifications.append(rpb2.Classification(labelset="ls", label="l1"))
    basic.computedmetadata.field_classifications.append(
        rpb2.FieldClassifications(field=rpb2.FieldID(field="f1", field_type=rpb2.FieldType.FILE))
    )
    basic.computedmetadata.field_classifications[0].classifications.append(
        rpb2.Classification(labelset="ls", label="l2")
    )
    extra = rpb2.Extra()
    extra.metadata.update({"key": "value"})
    resource = mock.Mock()
    resource.get_origin = AsyncMock(return_value=origin)
    resource.get_basic = AsyncMock(return_value=basic)
    field = mock.Mock()
    fcm = rpb2.FieldComputedMetadata()
    fcm.metadata.entities["processor"].entities.extend(
        [rpb2.FieldEntity(text="Barcelona", label="LOCATION")]
    )

    field.get_field_metadata = AsyncMock(return_value=fcm)
    resource.get_field = AsyncMock(return_value=field)
    resource.get_extra = AsyncMock(return_value=extra)
    with mock.patch(
        "nucliadb.search.search.chat.prompt.cache.get_resource",
        return_value=resource,
    ):
        paragraph_id = ParagraphId.from_string("r1/f/f1/0-10")
        context = chat_prompt.CappedPromptContext(max_size=int(1e6))
        context[paragraph_id.full()] = "Paragraph text"
        kbid = "foo"
        strategy = MetadataExtensionStrategy(types=list(MetadataExtensionType))
        await chat_prompt.extend_prompt_context_with_metadata(context, kbid, strategy, Metrics("foo"))

        text_block = context.output[paragraph_id.full()]
        assert "DOCUMENT METADATA AT ORIGIN" in text_block
        assert "DOCUMENT CLASSIFICATION LABELS" in text_block
        assert "DOCUMENT NAMED ENTITIES (NERs)" in text_block
        assert "DOCUMENT EXTRA METADATA" in text_block


async def test_prompt_context_image_context_builder():
    result_text = " ".join(["text"] * 10)
    find_results = KnowledgeboxFindResults(
        facets={},
        resources={
            "bmid": _create_find_result(
                FindParagraph(
                    id="bmid/f/file/0-1",
                    score=1,
                    score_type=SCORE_TYPE.BM25,
                    order=1,
                    text=result_text,
                    is_a_table=True,
                    reference="table_image_data",
                    page_with_visual=False,
                )
            ),
            "vecid": _create_find_result(
                FindParagraph(
                    id="vecid/f/file/0-1",
                    score=0,
                    score_type=SCORE_TYPE.VECTOR,
                    order=2,
                    text=result_text,
                    is_a_table=False,
                    reference="paragraph_image_data",
                    page_with_visual=False,
                )
            ),
            "both_id": _create_find_result(
                FindParagraph(
                    id="both_id/f/file/0-1",
                    score=2,
                    score_type=SCORE_TYPE.BOTH,
                    order=0,
                    text=result_text,
                    is_a_table=False,
                    reference="page_image_data",
                    page_with_visual=True,
                )
            ),
        },
    )

    # By default, no image strategies are provided so no images should be added
    builder = chat_prompt.PromptContextBuilder(
        kbid="kbid",
        ordered_paragraphs=get_ordered_paragraphs(find_results),
        user_context=["Carrots are orange"],
        image_strategies=[],
    )
    context = chat_prompt.CappedPromptContext(max_size=int(1e6))
    await builder._build_context_images(context)
    assert len(context.images) == 0

    # Test that the image strategies are applied correctly
    builder = chat_prompt.PromptContextBuilder(
        kbid="kbid",
        ordered_paragraphs=get_ordered_paragraphs(find_results),
        user_context=["Carrots are orange"],
        image_strategies=[PageImageStrategy(count=10), TableImageStrategy(), ParagraphImageStrategy()],
    )
    module = "nucliadb.search.search.chat.prompt"
    with (
        mock.patch(f"{module}.get_paragraph_page_number", return_value=1),
        mock.patch(
            f"{module}.get_page_image",
            return_value=Image(b64encoded="page_image_data", content_type="image/png"),
        ),
        mock.patch(
            f"{module}.get_paragraph_image",
            return_value=Image(b64encoded="table_image_data", content_type="image/png"),
        ),
    ):
        context = chat_prompt.CappedPromptContext(max_size=int(1e6))
        await builder._build_context_images(context)
        assert len(context.output) == 0
        assert len(context.images) == 6
        assert set(context.images.keys()) == {
            # The paragraph images
            "bmid/f/file/0-1",
            "both_id/f/file/0-1",
            "vecid/f/file/0-1",
            # The page images
            "bmid/f/file/1",
            "both_id/f/file/1",
            "vecid/f/file/1",
        }


async def test_prompt_context_builder_with_extra_image_context():
    image_content = base64.b64encode(b"my-image")
    user_image = Image(content_type="image/png", b64encoded=image_content)

    builder = chat_prompt.PromptContextBuilder(
        kbid="kbid",
        ordered_paragraphs=[],
        user_image_context=[user_image],
    )
    with patch("nucliadb.search.search.chat.prompt.default_prompt_context"):
        # context = chat_prompt.CappedPromptContext(max_size=int(1e6))
        _, _, context_images = await builder.build()

    assert len(context_images) == 1
    _, context_image = context_images.popitem()
    assert context_image == user_image
