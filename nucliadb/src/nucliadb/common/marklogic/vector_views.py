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
"""TDE views with an ANN index over the embeddings of each vectorset.

Embeddings are also stored in a per-vectorset collection that scopes its view. Templates live in the
Schemas database shared by every KB database, while view rows are per database, so a view only
holds the embeddings of its own KB. Views are keyed by vectorset id and dimension, as KBs may use
the same vectorset id with different dimensions.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from typing import TYPE_CHECKING

from nucliadb.common.maindb.marklogic_schema import SCHEMAS_DATABASE

if TYPE_CHECKING:
    from nucliadb.common.maindb.marklogic import MarkLogicDriver

SCHEMA_NAME = "nucliadb"

_ensured: set[tuple[str, int]] = set()
_lock = asyncio.Lock()


def view_name(vectorset_id: str, dimension: int) -> str:
    """
    >>> view_name("multilingual-2024-05-06", 1024)
    'embeddings_multilingual_2024_05_06_d1024_da99bad1'
    """
    safe_id = re.sub(r"[^A-Za-z0-9_]", "_", vectorset_id)
    # Keeps ids that normalize to the same name apart
    digest = hashlib.sha256(vectorset_id.encode()).hexdigest()[:8]
    return f"embeddings_{safe_id}_d{dimension}_{digest}"


def collection(vectorset_id: str, dimension: int) -> str:
    return view_name(vectorset_id, dimension)


def template_uri(vectorset_id: str, dimension: int) -> str:
    return f"/nucliadb/tde/{view_name(vectorset_id, dimension)}.json"


def template(vectorset_id: str, dimension: int) -> dict:
    return {
        "template": {
            "context": "/",
            "collections": [collection(vectorset_id, dimension)],
            "rows": [
                {
                    "schemaName": SCHEMA_NAME,
                    "viewName": view_name(vectorset_id, dimension),
                    "viewLayout": "sparse",
                    "columns": [
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


async def ensure_view(driver: MarkLogicDriver, database: str, vectorset_id: str, dimension: int) -> None:
    """Install the view template of a vectorset. Inserting a template reindexes the databases
    using it, so it is only inserted when missing."""
    key = (vectorset_id, dimension)
    if key in _ensured:
        return
    async with _lock:
        if key in _ensured:
            return
        uri = json.dumps(template_uri(vectorset_id, dimension))
        exists = await driver.client.eval(
            javascript=f"fn.docAvailable({uri})", params={"database": SCHEMAS_DATABASE}
        )
        if not exists or exists[0] is not True:
            # Templates are inserted from a database using the Schemas database
            await driver.client.eval(
                javascript=(
                    "declareUpdate(); const tde = require('/MarkLogic/tde.xqy'); "
                    f"tde.templateInsert({uri}, "
                    f"xdmp.toJSON({json.dumps(template(vectorset_id, dimension))}), "
                    "xdmp.defaultPermissions(), ['tde']);"
                ),
                params={"database": database},
            )
        _ensured.add(key)
