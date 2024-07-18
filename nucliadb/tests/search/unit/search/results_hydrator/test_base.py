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
from unittest.mock import patch

from nucliadb.common.external_index_providers.pinecone import PineconeQueryResults
from nucliadb.search.search.results_hydrator.base import hydrate_external
from nucliadb_models.resource import Resource
from nucliadb_models.search import KnowledgeboxFindResults
from nucliadb_utils.aiopynecone.models import QueryResponse, VectorMatch

MODULE = "nucliadb.search.search.results_hydrator.base"


async def test_hydrate_external():
    with (
        patch(f"{MODULE}.managed_serialize", return_value=Resource(id="rid")),
        patch(f"{MODULE}.paragraphs.get_paragraph_text", return_value="some text"),
        patch(f"{MODULE}.get_driver"),
    ):
        retrieval_results = KnowledgeboxFindResults(resources={})
        query_results = PineconeQueryResults(
            results=QueryResponse(
                matches=[
                    VectorMatch(
                        id="rid/f/field/0/0-10",
                        score=0.8,
                        values=None,
                        metadata={"labels": ["/t/text/label"], "access_groups": ["ag1", "ag2"]},
                    )
                ]
            )
        )
        await hydrate_external(
            retrieval_results,
            query_results=query_results,
            kbid="kbid",
        )

        resources = retrieval_results.resources
        assert len(resources) == 1
        assert resources["rid"].id == "rid"
        fields = resources["rid"].fields
        assert len(fields) == 1
        paragraphs = fields["rid/f/field"].paragraphs
        assert len(paragraphs) == 1
        assert paragraphs["rid/f/field/0/0-10"].text == "some text"