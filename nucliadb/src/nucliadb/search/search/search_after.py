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

from base64 import b64decode, b64encode

from nidx_protos.nodereader_pb2 import SearchResponse
from pydantic import BaseModel, Field

from nucliadb.search.search.query_parser.models import SearchAfter


class SearchAfterToken(BaseModel):
    # Indicates the first result to fetch from the index
    after: SearchAfter | None = None

    # Indicates the results already shown that should be skipped
    skip: list[str] = Field(default_factory=list)

    def encode(self) -> str:
        return b64encode(self.model_dump_json().encode()).decode()

    @staticmethod
    def decode(token: str) -> "SearchAfterToken":
        return SearchAfterToken.model_validate_json(b64decode(token))


def build_search_after_token(response: SearchResponse, shown_results: set[str]) -> str:
    token = SearchAfterToken()

    for paragraph in response.paragraph.results:
        if paragraph.paragraph in shown_results:
            # Mark top consecutive results to be skipped with search_after
            token.after = SearchAfter(
                score=paragraph.score.bm25, shard=paragraph.shard_id, docaddr=paragraph.score.docaddr
            )
            # This results is skipped by after, no need to include in skip list
            shown_results.remove(paragraph.paragraph)
        else:
            break

    token.skip += shown_results

    return token.encode()
