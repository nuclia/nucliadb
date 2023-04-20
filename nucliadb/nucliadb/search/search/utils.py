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
from typing import Optional

from fastapi import HTTPException

from nucliadb_models.search import SortField, SortOptions, SortOrder

INDEX_SORTABLE_FIELDS = [
    SortField.CREATED,
    SortField.MODIFIED,
]


def parse_sort_options(
    query: str, advanced_query: Optional[str] = None, sort: Optional[SortOptions] = None
) -> SortOptions:
    is_empty_query = len(query) == 0 and (
        advanced_query is None or len(advanced_query) == 0
    )
    if is_empty_query:
        if sort is None:
            return SortOptions(
                field=SortField.CREATED,
                order=SortOrder.DESC,
                limit=None,
            )
        if sort.field not in INDEX_SORTABLE_FIELDS:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Empty query can only be sorted by '{SortField.CREATED}' or"
                    f" '{SortField.MODIFIED}' and sort limit won't be applied"
                ),
            )
        sort.limit = None
        return sort
    else:
        if sort is None:
            return SortOptions(
                field=SortField.SCORE,
                order=SortOrder.DESC,
                limit=None,
            )
        if sort.field not in INDEX_SORTABLE_FIELDS and sort.limit is None:
            raise HTTPException(
                status_code=422,
                detail=f"Sort by '{sort.field}' requires setting a sort limit",
            )
        return sort
