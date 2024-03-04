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
import nucliadb_sdk
from nucliadb_models.search import KnowledgeboxFindResults, SummarizeRequest


def test_summarize(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    results: KnowledgeboxFindResults = sdk.find(kbid=docs_dataset, query="love")
    resource_uuids = [uuid for uuid in results.resources.keys()]

    response = sdk.summarize(
        kbid=docs_dataset, resources=[resource_uuids[0]], generative_model="everest"
    )
    assert response.summary == "global summary"

    content = SummarizeRequest(resources=[resource_uuids[0]])
    response = sdk.summarize(kbid=docs_dataset, content=content)
    assert response.summary == "global summary"
