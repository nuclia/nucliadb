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
"""TDE views with an ANN index over the embeddings of each vector dimension.

A view column has a fixed dimension, so embeddings are also stored in a per-dimension collection
that scopes the view. Templates live in the Schemas database shared by every KB database.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

from nucliadb.common.maindb.marklogic_schema import SCHEMAS_DATABASE

if TYPE_CHECKING:
    from nucliadb.common.maindb.marklogic import MarkLogicDriver

SCHEMA_NAME = "nucliadb"

_ensured: set[int] = set()
_lock = asyncio.Lock()


def collection(dimension: int) -> str:
    return f"embeddings_d{dimension}"


def view_name(dimension: int) -> str:
    return collection(dimension)


def template_uri(dimension: int) -> str:
    return f"/nucliadb/tde/{view_name(dimension)}.json"


def template(dimension: int) -> dict:
    return {
        "template": {
            "context": "/",
            "collections": [collection(dimension)],
            "rows": [
                {
                    "schemaName": SCHEMA_NAME,
                    "viewName": view_name(dimension),
                    "viewLayout": "sparse",
                    "columns": [
                        {"name": "model_id", "scalarType": "string", "val": "model_id"},
                        {
                            "name": "vector",
                            "scalarType": "vector",
                            "val": "vec:vector(vector)",
                            "dimension": dimension,
                            "annIndexed": True,
                            "annDistance": "cosine",
                            # A malformed vector must never make the document write fail
                            "invalidValues": "ignore",
                            "nullable": True,
                        },
                    ],
                }
            ],
        }
    }


async def ensure_view(driver: MarkLogicDriver, database: str, dimension: int) -> None:
    """Install the view template for `dimension`. Inserting a template reindexes the databases
    using it, so it is only inserted when missing."""
    if dimension in _ensured:
        return
    async with _lock:
        if dimension in _ensured:
            return
        uri = json.dumps(template_uri(dimension))
        exists = await driver.client.eval(
            javascript=f"fn.docAvailable({uri})", params={"database": SCHEMAS_DATABASE}
        )
        if not exists or exists[0] is not True:
            # Templates are inserted from a database using the Schemas database
            await driver.client.eval(
                javascript=(
                    "declareUpdate(); const tde = require('/MarkLogic/tde.xqy'); "
                    f"tde.templateInsert({uri}, xdmp.toJSON({json.dumps(template(dimension))}), "
                    "xdmp.defaultPermissions(), ['tde']);"
                ),
                params={"database": database},
            )
        _ensured.add(dimension)
