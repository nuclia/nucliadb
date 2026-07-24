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

from nidx_protos.nodereader_pb2 import (
    SearchResponse,
)

from nucliadb.search.search.merge import ResourceSearchResults, merge_paragraphs_results


@patch("nucliadb.search.search.merge.augment_paragraphs", return_value={})
async def test_str_model(_mock):
    # make sure __str__ works as advertised
    res = await merge_paragraphs_results(SearchResponse(), 1, "kbid", False, 1)
    assert str(res) == res.model_dump_json()


@patch("nucliadb.search.search.merge.augment_paragraphs", return_value={})
async def test_str_model_fallback(_mock):
    with patch.object(ResourceSearchResults, "model_dump_json", side_effect=Exception("ERROR")):
        res = await merge_paragraphs_results(SearchResponse(), 1, "kbid", False, 1)
        assert "sentences=None" in str(res)
