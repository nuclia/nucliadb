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

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId
from nucliadb.search.search.hydrator import (
    ResourceHydrationOptions,
    TextBlockHydrationOptions,
    hydrate_resource_metadata_and_update_find_resources,
    hydrate_text_block_and_update_find_paragraph,
)
from nucliadb_models.resource import Resource
from nucliadb_models.search import (
    SCORE_TYPE,
    FindResource,
    TextPosition,
)

MODULE = "nucliadb.search.search.hydrator"


async def test_hydrate_text_block():
    with (
        patch(f"{MODULE}.paragraphs.get_paragraph_text", return_value="some text"),
        patch(f"{MODULE}.get_driver"),
    ):
        text_block = TextBlockMatch(
            paragraph_id=ParagraphId.from_string("rid/f/field/0/0-10"),
            position=TextPosition(index=0, start=0, end=10),
            score=0.8,
            score_type=SCORE_TYPE.VECTOR,
            order=3,
            fuzzy_search=False,
            paragraph_labels=["/t/text/label"],
        )

        field_paragraphs = {}
        await hydrate_text_block_and_update_find_paragraph(
            "kbid", text_block, TextBlockHydrationOptions(), field_paragraphs
        )

        assert text_block.text == "some text"

        assert len(field_paragraphs) == 1
        find_paragraph = field_paragraphs["rid/f/field/0/0-10"]
        assert find_paragraph.text == "some text"
        assert find_paragraph.score == 0.8
        assert find_paragraph.score_type == SCORE_TYPE.VECTOR
        assert find_paragraph.order == 3
        assert find_paragraph.labels == ["/t/text/label"]


async def test_hydrate_resource_metadata():
    with (
        patch(f"{MODULE}.managed_serialize", return_value=Resource(id="rid", slug="my-resource")),
        patch(f"{MODULE}.get_driver"),
    ):
        find_resources = {"rid": FindResource(id="aaa", fields={})}
        await hydrate_resource_metadata_and_update_find_resources(
            "kbid", "rid", ResourceHydrationOptions(), find_resources
        )

        assert len(find_resources) == 1
        find_resource = find_resources["rid"]
        assert find_resource.id == "rid"
        assert find_resource.slug == "my-resource"
