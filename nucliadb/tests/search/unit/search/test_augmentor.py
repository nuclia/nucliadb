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

from nucliadb.common.ids import ParagraphId
from nucliadb.models.internal.augment import Metadata, ParagraphText
from nucliadb.search.augmentor.paragraphs import db_augment_paragraph
from nucliadb.search.augmentor.resources import augment_resource_deep
from nucliadb.search.search.hydrator import ResourceHydrationOptions
from nucliadb_models.resource import Resource

MODULE = "nucliadb.search.augmentor"


async def test_augment_text():
    with (
        patch(f"{MODULE}.paragraphs.get_paragraph_from_full_text", return_value="some text"),
    ):
        resource = Mock()
        field = Mock()
        augmented = await db_augment_paragraph(
            resource,
            field,
            paragraph_id=ParagraphId.from_string("rid/f/field/0/0-10"),
            select=[ParagraphText()],
            metadata=Metadata(
                is_an_image=False,
                is_a_table=False,
                source_file=None,
                page=None,
                in_page_with_visual=None,
            ),
        )
        assert augmented.text is not None
        assert augmented.text == "some text"


async def test_augment_resource():
    with (
        patch(f"{MODULE}.resources.cache.get_resource"),
        patch(
            f"{MODULE}.resources.serialize_resource", return_value=Resource(id="rid", slug="my-resource")
        ),
    ):
        augmented = await augment_resource_deep("kbid", "rid", opts=ResourceHydrationOptions())
        assert augmented == Resource(id="rid", slug="my-resource")
