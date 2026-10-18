import pytest

from nucliadb.common.marklogic.uris import MarkLogicURIs


def test_chunk_uri_uses_field_relative_paragraph_id():
    assert (
        MarkLogicURIs.chunk("rid", "t", "body", "rid/t/body/12-34")
        == "/resources/rid/fields/t/body/chunks/12-34/data.json"
    )


def test_split_chunk_uri_preserves_subfield():
    assert (
        MarkLogicURIs.chunk("rid", "c", "chat", "rid/c/chat/message-1/12-34")
        == "/resources/rid/fields/c/chat/chunks/message-1/12-34/data.json"
    )


def test_embedding_uri_preserves_sentence_index():
    assert (
        MarkLogicURIs.embedding("rid", "t", "body", "model", "rid/t/body/2/12-34")
        == "/resources/rid/fields/t/body/embeddings/model/2/12-34/data.json"
    )


def test_split_embedding_uri_preserves_subfield_and_sentence_index():
    assert (
        MarkLogicURIs.embedding("rid", "c", "chat", "model", "rid/c/chat/message-1/2/12-34")
        == "/resources/rid/fields/c/chat/embeddings/model/message-1/2/12-34/data.json"
    )


@pytest.mark.parametrize(
    "chunk_id",
    ["other/t/body/12-34", "rid/t/body/message/extra/12-34", "rid/t/body/not-a-range"],
)
def test_chunk_uri_rejects_invalid_chunk_id(chunk_id):
    with pytest.raises(ValueError):
        MarkLogicURIs.chunk("rid", "t", "body", chunk_id)


def test_embedding_uri_rejects_id_without_sentence_index():
    with pytest.raises(ValueError):
        MarkLogicURIs.embedding("rid", "t", "body", "model", "rid/t/body/12-34")
