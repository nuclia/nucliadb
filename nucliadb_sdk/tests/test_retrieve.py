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
from nucliadb_models.retrieval import KeywordQuery, Query, RetrievalRequest, RetrievalResponse


def test_retrieve(docs_dataset: str, sdk: nucliadb_sdk.NucliaDB):
    results: RetrievalResponse = sdk.retrieve(
        kbid=docs_dataset, content=RetrievalRequest(query=Query(keyword=KeywordQuery(query="love")))
    )
    assert len(results.matches) > 0
