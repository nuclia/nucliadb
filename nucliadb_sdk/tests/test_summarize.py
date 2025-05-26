# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
