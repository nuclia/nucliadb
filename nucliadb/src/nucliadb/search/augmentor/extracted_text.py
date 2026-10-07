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
from contextvars import ContextVar

from nidx_protos.nidx_pb2 import ExtractedTextsResponse

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.search.augmentor.metrics import augmentor_observer


class ExtractedTexts:
    def __init__(self, nidx_responses: list[ExtractedTextsResponse]):
        self.responses = nidx_responses

    def get_field_text(self, id: FieldId) -> str | None:
        text = None
        for response in self.responses:
            text = response.fields.get(id.full_without_subfield())
            if text:
                break
        return text or None

    def get_paragraph_text(self, id: ParagraphId) -> str | None:
        text = None
        for response in self.responses:
            text = response.paragraphs.get(id.full())
            if text:
                break
        return text


nidx_et_cache: ContextVar[ExtractedTexts | None] = ContextVar("nidx_et_cache", default=None)


@augmentor_observer.wrap({"type": "nidx_extracted_texts"})
async def extracted_texts(
    kbid: str, fields: set[FieldId], paragraphs: set[ParagraphId]
) -> ExtractedTexts | None:
    # TODO(Marklogic): Replace Nidx searcher with Marklogic implementation
    return None
