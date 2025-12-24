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
from pydantic import BaseModel

from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName
from nucliadb_models.search import ResourceProperties


class ResourceHydrationOptions(BaseModel):
    """
    Options for hydrating resources.
    """

    show: list[ResourceProperties] = []
    extracted: list[ExtractedDataTypeName] = []
    field_type_filter: list[FieldTypeName] = []


class TextBlockHydrationOptions(BaseModel):
    """
    Options for hydrating text blocks (aka paragraphs).
    """

    # whether to highlight the text block with `<mark>...</mark>` tags or not
    highlight: bool = False

    # list of exact matches to highlight
    ematches: list[str] | None = None

    # If true, only hydrate the text block if its text is not already populated
    only_hydrate_empty: bool = False
