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
from nucliadb_models.search import KnowledgeboxFindResults


def test_find_resource(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    results: KnowledgeboxFindResults = sdk.find(kbid=docs_dataset, query="love")
    assert results.total == 10
    paragraphs = 0
    for res in results.resources.values():
        for field in res.fields.values():
            paragraphs += len(field.paragraphs)

    assert paragraphs == 10
