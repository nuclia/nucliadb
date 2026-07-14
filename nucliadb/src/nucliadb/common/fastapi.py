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

from typing import Annotated, Any, Callable

from fastapi import Depends, HTTPException, Path

from nucliadb.common.ids import valid_kbid, valid_rid

# ---------------------------------------------------------------------------
# FastAPI path-parameter dependencies that return 404 for non-UUID values.
#
# Use these instead of plain ``str`` for ``kbid`` and ``rid`` path parameters
# so that requests with syntactically invalid identifiers are rejected at the
# API layer before reaching the database, while preserving the existing
# behaviour of returning 404 (not 422) for bad input.
#
# Usage::
#
#     from nucliadb.common.fastapi import KbId, RId
#
#     @router.get("/kb/{kbid}/resource/{rid}")
#     async def get_resource(kbid: KbId, rid: RId): ...
# ---------------------------------------------------------------------------


def _uuid_path_param(name: str, not_found_detail: str, validator_func: Callable[[str], bool]) -> Any:
    """Return a FastAPI ``Depends`` callable that validates *name* as a UUID."""

    def _validate(value: str = Path(alias=name)) -> str:
        if not validator_func(value):
            raise HTTPException(status_code=404, detail=not_found_detail)
        return value

    _validate.__name__ = f"validate_{name}"
    return Depends(_validate)


KbId = Annotated[str, _uuid_path_param("kbid", "Knowledge box not found. UUID expected.", valid_kbid)]
RId = Annotated[str, _uuid_path_param("rid", "Resource not found. UUID expected.", valid_rid)]
PathRId = Annotated[str, _uuid_path_param("path_rid", "Resource not found. UUID expected.", valid_rid)]
