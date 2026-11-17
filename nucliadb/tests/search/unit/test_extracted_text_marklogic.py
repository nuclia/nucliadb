from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.search.augmentor import extracted_text as module


async def test_extracted_text_reads_chunks_and_falls_back_to_field_text(monkeypatch):
    field = FieldId(rid="rid", type="t", key="body")
    indexed = ParagraphId(field_id=field, paragraph_start=0, paragraph_end=3)
    unindexed = ParagraphId(field_id=field, paragraph_start=4, paragraph_end=7)
    missing_field = FieldId(rid="missing", type="t", key="body")
    missing = ParagraphId(field_id=missing_field, paragraph_start=0, paragraph_end=3)
    txn = Mock()

    @asynccontextmanager
    async def transaction(*, kbid):
        assert kbid == "kb"
        yield txn

    monkeypatch.setattr(module, "get_driver", lambda: Mock(ro_transaction=transaction))
    read_many = AsyncMock(
        return_value={
            MarkLogicURIs.field_processed("rid", "body", "t"): {"extracted_text": "foo bar"},
            MarkLogicURIs.chunk("rid", "t", "body", indexed.full()): {"text": "indexed text"},
        }
    )
    monkeypatch.setattr(module.documents, "read_many", read_many)

    result = await module.extracted_texts("kb", {field, missing_field}, {indexed, unindexed, missing})

    assert result is not None
    assert result.get_field_text(field) == "foo bar"
    assert result.get_field_text(missing_field) is None
    assert result.get_paragraph_text(indexed) == "indexed text"
    assert result.get_paragraph_text(unindexed) == "bar"
    assert result.get_paragraph_text(missing) is None
    read_many.assert_awaited_once()
    assert read_many.call_args.args[0] is txn


@pytest.mark.parametrize("request_field", [False, True])
async def test_found_chunks_only_read_explicitly_requested_fields(monkeypatch, request_field):
    field = FieldId(rid="rid", type="t", key="body")
    paragraph = ParagraphId(field_id=field, paragraph_start=0, paragraph_end=3)
    chunk_uri = MarkLogicURIs.chunk("rid", "t", "body", paragraph.full())
    field_uri = MarkLogicURIs.field_processed("rid", "body", "t")
    txn = Mock()

    @asynccontextmanager
    async def transaction(*, kbid):
        assert kbid == "kb"
        yield txn

    monkeypatch.setattr(module, "get_driver", lambda: Mock(ro_transaction=transaction))
    contents = {chunk_uri: {"text": "foo"}}
    if request_field:
        contents[field_uri] = {"extracted_text": "foo bar"}
    read_many = AsyncMock(return_value=contents)
    monkeypatch.setattr(module.documents, "read_many", read_many)

    result = await module.extracted_texts("kb", {field} if request_field else set(), {paragraph})

    assert result is not None
    assert result.get_paragraph_text(paragraph) == "foo"
    assert result.get_field_text(field) == ("foo bar" if request_field else None)
    read_many.assert_awaited_once()
    assert read_many.call_args.args[0] is txn
    assert set(read_many.call_args.args[1]) == ({chunk_uri, field_uri} if request_field else {chunk_uri})


@pytest.mark.parametrize("fallback_found", [False, True])
async def test_missing_chunks_read_deduplicated_fallback_fields(monkeypatch, fallback_found):
    field = FieldId(rid="rid", type="t", key="body")
    indexed_field = FieldId(rid="indexed", type="t", key="body")
    indexed = ParagraphId(field_id=indexed_field, paragraph_start=0, paragraph_end=3)
    first = ParagraphId(field_id=field, paragraph_start=0, paragraph_end=3)
    second = ParagraphId(field_id=field, paragraph_start=4, paragraph_end=7)
    paragraphs = {indexed, first, second}
    chunk_uris = {
        MarkLogicURIs.chunk(
            paragraph.field_id.rid, paragraph.field_id.type, paragraph.field_id.key, paragraph.full()
        )
        for paragraph in paragraphs
    }
    indexed_uri = MarkLogicURIs.chunk("indexed", "t", "body", indexed.full())
    fallback_uri = MarkLogicURIs.field_processed("rid", "body", "t")
    txn = Mock()

    @asynccontextmanager
    async def transaction(*, kbid):
        assert kbid == "kb"
        yield txn

    monkeypatch.setattr(module, "get_driver", lambda: Mock(ro_transaction=transaction))
    read_many = AsyncMock(
        side_effect=[
            {indexed_uri: {"text": "indexed text"}},
            {fallback_uri: {"extracted_text": "foo bar"}} if fallback_found else {},
        ]
    )
    monkeypatch.setattr(module.documents, "read_many", read_many)

    result = await module.extracted_texts("kb", set(), paragraphs)

    assert result is not None
    assert result.get_paragraph_text(indexed) == "indexed text"
    assert result.get_paragraph_text(first) == ("foo" if fallback_found else None)
    assert result.get_paragraph_text(second) == ("bar" if fallback_found else None)
    assert result.get_field_text(field) is None
    assert read_many.await_count == 2
    initial_call, fallback_call = read_many.await_args_list
    assert initial_call.args[0] is txn
    assert set(initial_call.args[1]) == chunk_uris
    assert fallback_call.args[0] is txn
    assert fallback_call.args[1] == [fallback_uri]
