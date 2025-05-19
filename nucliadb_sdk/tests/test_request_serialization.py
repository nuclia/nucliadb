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

import json
from unittest.mock import Mock, patch

import nucliadb_sdk
from nucliadb_models.search import FindOptions, FindRequest, KnowledgeboxFindResults


def test_find_request_serialization() -> None:
    sdk = nucliadb_sdk.NucliaDB(region="on-prem", url="http://fake:8080")

    with patch.object(
        sdk,
        "_request",
        return_value=Mock(content=KnowledgeboxFindResults(resources={}).model_dump_json()),
    ) as spy:
        req = FindRequest(query="love", features=[FindOptions.RELATIONS])

        sdk.find(kbid="kbid", content=req)

        sent = json.loads(spy.call_args.kwargs["content"])

        assert sent == {"query": "love", "features": [FindOptions.RELATIONS.value]}
        assert sent == req.model_dump(exclude_unset=True)
