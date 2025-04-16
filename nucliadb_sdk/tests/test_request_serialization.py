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

import json
from unittest.mock import Mock, patch

import nucliadb_sdk
from nucliadb_models.search import FindRequest, KnowledgeboxFindResults, SearchOptions


def test_find_request_serialization() -> None:
    sdk = nucliadb_sdk.NucliaDB(region="on-prem", url="http://fake:8080")

    with patch.object(
        sdk,
        "_request",
        return_value=Mock(content=KnowledgeboxFindResults(resources={}).model_dump_json()),
    ) as spy:
        req = FindRequest(query="love", features=[SearchOptions.RELATIONS])

        sdk.find(kbid="kbid", content=req)

        sent = json.loads(spy.call_args.kwargs["content"])

        assert sent == {"query": "love", "features": [SearchOptions.RELATIONS.value]}
        assert sent == req.model_dump(exclude_unset=True)
