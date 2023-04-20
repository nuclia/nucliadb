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

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Iterator, List, Optional

from nucliadb_models.search import KnowledgeboxSearchResults
from nucliadb_sdk.client import NucliaDBClient

logger = logging.getLogger(__name__)


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
    field: str
    field_type: str


class SearchResult:
    inner_search_results: KnowledgeboxSearchResults

    def __init__(
        self, inner_search_results: KnowledgeboxSearchResults, client: NucliaDBClient
    ):
        self.inner_search_results = inner_search_results
        self.client = client

    def _get_result(
        self,
        *,
        rid: str,
        field_type: str,
        field: str,
        score: float,
        score_type: ScoreType,
    ) -> Optional[SearchResource]:
        resource = self.client.get_resource(rid)
        if field_type == "t":
            text = resource.data.texts[field].value.body
        elif field_type == "a":
            text = resource.data.generics[field].value
        elif field_type == "f":
            filename = resource.data.files[field].value.file.filename
            text = f"File: {filename}"
        elif field_type == "l":
            uri = resource.data.links[field].value.uri
            text = f"Link: {uri}"
        else:
            logger.warning(f"Unsupported field type: {field_type} on field {field}")
            return None

        classifications = [
            classification.label
            for classification in resource.usermetadata.classifications
        ]
        return SearchResource(
            text=text,
            field=field,
            field_type=field_type,
            labels=classifications,
            score=score,
            key=rid,
            score_type=score_type,
        )

    def __iter__(self) -> Iterator[SearchResource]:
        if self.inner_search_results.fulltext is not None:
            for fts in self.inner_search_results.fulltext.results:
                result = self._get_result(
                    rid=fts.rid,
                    field_type=fts.field_type,
                    field=fts.field,
                    score=fts.score,
                    score_type=ScoreType.BM25,
                )
                if result is not None:
                    yield result

        if self.inner_search_results.sentences is not None:
            for sentence in self.inner_search_results.sentences.results:
                result = self._get_result(
                    rid=sentence.rid,
                    field_type=sentence.field_type,
                    field=sentence.field,
                    score=sentence.score,
                    score_type=ScoreType.COSINE,
                )
                if result is not None:
                    yield result

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
