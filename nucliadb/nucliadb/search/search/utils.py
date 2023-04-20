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
from fastapi import HTTPException

from nucliadb_models.search import SearchRequest, SortField, SortOptions, SortOrder

INDEX_SORTABLE_FIELDS = [
    SortField.CREATED,
    SortField.MODIFIED,
]


def parse_sort_options(item: SearchRequest) -> SortOptions:
    if is_empty_query(item):
        if item.sort is None:
            sort_options = SortOptions(
                field=SortField.CREATED,
                order=SortOrder.DESC,
                limit=None,
            )
        elif item.sort.field not in INDEX_SORTABLE_FIELDS:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Empty query can only be sorted by '{SortField.CREATED}' or"
                    f" '{SortField.MODIFIED}' and sort limit won't be applied"
                ),
            )
        else:
            sort_options = item.sort
    else:
        if item.sort is None:
            sort_options = SortOptions(
                field=SortField.SCORE,
                order=SortOrder.DESC,
                limit=None,
            )
        elif item.sort.field not in INDEX_SORTABLE_FIELDS and item.sort.limit is None:
            raise HTTPException(
                status_code=422,
                detail=f"Sort by '{item.sort.field}' requires setting a sort limit",
            )
        else:
            sort_options = item.sort

    return sort_options


def is_empty_query(request: SearchRequest) -> bool:
    return len(request.query) == 0 and (
        request.advanced_query is None or len(request.advanced_query) == 0
    )
