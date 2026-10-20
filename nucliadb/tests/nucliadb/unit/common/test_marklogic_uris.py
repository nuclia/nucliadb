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
