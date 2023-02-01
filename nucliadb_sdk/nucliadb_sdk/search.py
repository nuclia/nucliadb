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

from dataclasses import dataclass
from enum import Enum
from typing import Iterator, List

from nucliadb_models.search import KnowledgeboxSearchResults
from nucliadb_sdk.client import NucliaDBClient


class ScoreType(str, Enum):
    BM25 = "BM25"
    COSINE = "COSINE"


@dataclass
class SearchResource:
    key: str
    text: str
    labels: List[str]
    score: float
    score_type: ScoreType


class SearchResult:
    inner_search_results: KnowledgeboxSearchResults

    def __init__(
        self, inner_search_results: KnowledgeboxSearchResults, client: NucliaDBClient
    ):
        self.inner_search_results = inner_search_results
        self.client = client

    def __iter__(self) -> Iterator[SearchResource]:
        if self.inner_search_results.fulltext is not None:
            for fts in self.inner_search_results.fulltext.results:
                resource = self.client.get_resource(fts.rid)
                if fts.field_type == "t":
                    text = resource.data.texts[fts.field].value.body
                classifications = [
                    classification.label
                    for classification in resource.usermetadata.classifications
                ]
                yield SearchResource(
                    text=text,
                    labels=classifications,
                    score=fts.score,
                    key=fts.rid,
                    score_type=ScoreType.BM25,
                )

        if self.inner_search_results.sentences is not None:
            for sentence in self.inner_search_results.sentences.results:
                resource = self.client.get_resource(sentence.rid)
                if sentence.field_type == "t":
                    text = resource.data.texts[sentence.field].value.body
                classifications = [
                    classification.label
                    for classification in resource.usermetadata.classifications
                ]
                yield SearchResource(
                    text=text,
                    labels=classifications,
                    score=sentence.score,
                    key=sentence.rid,
                    score_type=ScoreType.COSINE,
                )

    @property
    def fulltext(self):
        return self.inner_search_results.fulltext

    @property
    def sentences(self):
        return self.inner_search_results.sentences

    @property
    def paragraphs(self):
        return self.inner_search_results.paragraphs

    @property
    def resources(self):
        return self.inner_search_results.resources

    @property
    def relations(self):
        return self.inner_search_results.relations
