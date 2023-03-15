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
from unittest.mock import Mock, patch

import pytest
from nucliadb_protos.knowledgebox_pb2 import Synonyms

from nucliadb.search.search.synonyms import apply_synonyms_to_request


class TestApplySynonymsToRequest:
    @pytest.fixture
    def get_synonyms(self):
        with patch(
            "nucliadb.search.search.synonyms.get_kb_synonyms"
        ) as get_kb_synonyms:
            synonyms = Synonyms()
            synonyms.terms["planet"].synonyms.extend(["earth", "globe"])
            get_kb_synonyms.return_value = synonyms
            yield get_kb_synonyms

    @pytest.mark.asyncio
    async def test_not_applies_if_empty_body(self, get_synonyms):
        search_request = Mock(body="")
        await apply_synonyms_to_request(search_request, "kbid")

        get_synonyms.assert_not_awaited()
        search_request.ClearField.assert_not_called()

    @pytest.mark.asyncio
    async def test_not_applies_if_synonyms_object_not_found(self, get_synonyms):
        get_synonyms.return_value = None
        request = Mock(body="planet")

        await apply_synonyms_to_request(request, "kbid")

        request.ClearField.assert_not_called()
        get_synonyms.assert_awaited_once_with("kbid")

    @pytest.mark.asyncio
    async def test_not_applies_if_synonyms_not_found_for_query(self, get_synonyms):
        request = Mock(body="foobar")

        await apply_synonyms_to_request(request, "kbid")

        request.ClearField.assert_not_called()

        request.body = "planet"
        await apply_synonyms_to_request(request, "kbid")

        request.ClearField.assert_called_once_with("body")
        assert request.advanced_query == "planet OR earth OR globe"
