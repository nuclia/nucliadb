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
"""MarkLogic database provisioning shared by schema migrations and KnowledgeBox creation."""

from __future__ import annotations

import zlib
from collections.abc import Sequence

from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.marklogic.admin_client import AdminClient

STRING_COLLATION = "http://marklogic.com/collation/codepoint"

DATABASE_PROPERTIES = {
    "collection-lexicon": True,
    "uri-lexicon": True,
    "word-searches": True,
    "trailing-wildcard-searches": True,
    "three-character-searches": True,
    "fast-element-character-searches": True,
    "fast-element-trailing-wildcard-searches": True,
}

# The system database holds global keys and the KnowledgeBox registry.
SYSTEM_PATH_INDEXES = (
    MarkLogicIndexPaths.MAINDB_KEY,
    MarkLogicIndexPaths.SLUG,
)

# Each KnowledgeBox database holds its own keys, resources, fields and conversations.
KB_PATH_INDEXES = (
    MarkLogicIndexPaths.MAINDB_KEY,
    MarkLogicIndexPaths.RID,
    MarkLogicIndexPaths.MD5,
    MarkLogicIndexPaths.SHARD,
    MarkLogicIndexPaths.SLUG,
    MarkLogicIndexPaths.TITLE,
)


def _path_index(path: str) -> dict:
    return {
        "scalar-type": "string",
        "path-expression": path,
        "collation": STRING_COLLATION,
        "range-value-positions": False,
        "invalid-values": "ignore",
    }


async def _ensure_forest(client: AdminClient, database: str) -> None:
    forest_name = f"{database}-forest"
    if await client.get_forest(forest_name) is not None:
        return
    hosts = sorted(await client.list_hosts())
    if not hosts:
        raise RuntimeError("MarkLogic returned no hosts for forest creation")
    # Stable hash so forests are spread evenly across the cluster instead of piling on one host
    host = hosts[zlib.crc32(database.encode()) % len(hosts)]
    await client.create_forest(forest_name, host=host, database=database)


async def _reconcile_properties(client: AdminClient, database: str, path_indexes: Sequence[str]) -> None:
    properties = await client.get_database_properties(database)
    changed: dict = {key: value for key, value in DATABASE_PROPERTIES.items() if not properties.get(key)}
    existing = properties.get("range-path-index", [])
    missing = [
        path
        for path in path_indexes
        if not any(item.get("path-expression") == path for item in existing)
    ]
    if missing:
        changed["range-path-index"] = [*existing, *(_path_index(path) for path in missing)]
    if not changed:
        return
    await client.update_database_properties(database, changed)


async def ensure_database(client: AdminClient, database: str, path_indexes: Sequence[str]) -> None:
    """Create the database with its forest and indexes, or reconcile an existing one."""
    if await client.get_database(database) is None:
        await client.create_database(
            {
                "database-name": database,
                "schema-database": "Schemas",
                "triggers-database": "Triggers",
                **DATABASE_PROPERTIES,
                "range-path-index": [_path_index(path) for path in path_indexes],
            }
        )
    else:
        await _reconcile_properties(client, database, path_indexes)
    await _ensure_forest(client, database)
