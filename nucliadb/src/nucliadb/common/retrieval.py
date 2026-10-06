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

from pydantic import BaseModel

from nucliadb.common.ids import ParagraphId
from nucliadb_models.retrieval import Score
from nucliadb_models.search import SCORE_TYPE, Relations, TextPosition
from nucliadb_protos import resources_pb2

# /k/ocr
_OCR_LABEL = (
    f"/k/{resources_pb2.Paragraph.TypeParagraph.Name(resources_pb2.Paragraph.TypeParagraph.OCR).lower()}"
)
# /k/inception
_INCEPTION_LABEL = f"/k/{resources_pb2.Paragraph.TypeParagraph.Name(resources_pb2.Paragraph.TypeParagraph.INCEPTION).lower()}"


class ScoredTextBlock(BaseModel):
    paragraph_id: ParagraphId
    score_type: SCORE_TYPE

    scores: list[Score]

    @property
    def score(self) -> float:
        return self.current_score.score

    @property
    def current_score(self) -> Score:
        assert len(self.scores) > 0, "text block matches must be scored"
        return self.scores[-1]


class TextBlockMatch(ScoredTextBlock):
    """
    Model a text block/paragraph retrieved from an external index with all the information
    needed in order to later hydrate retrieval results.
    """

    position: TextPosition
    order: int
    page_with_visual: bool = False
    fuzzy_search: bool
    is_a_table: bool = False
    representation_file: str | None = None
    paragraph_labels: list[str] = []
    field_labels: list[str] = []
    text: str | None = None
    relevant_relations: Relations | None = None

    @property
    def is_an_image(self) -> bool:
        return _OCR_LABEL in self.paragraph_labels or _INCEPTION_LABEL in self.paragraph_labels
