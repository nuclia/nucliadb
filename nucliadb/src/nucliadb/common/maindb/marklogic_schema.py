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

import logging
import zlib
from collections.abc import Sequence

from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.marklogic.admin_client import AdminClient

logger = logging.getLogger(__name__)

STRING_COLLATION = "http://marklogic.com/collation/codepoint"
SCHEMAS_DATABASE = "Schemas"

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
    MarkLogicIndexPaths.FACETS,
    MarkLogicIndexPaths.RESOURCE_FACETS,
    MarkLogicIndexPaths.ACCESS_GROUPS,
    "/relations/source_normalized",
    "/relations/destination_normalized",
)

# Expands fuzzy keyword patterns into indexed words, so fuzzy matches get BM25 scores
KB_WORD_LEXICON_PROPERTIES = ("text",)

# Kept out of the universal index, so every float of the chunk embeddings is not indexed
KB_EXCLUDED_PATHS = ("//vector",)


def _word_lexicon(prop: str) -> dict:
    return {"namespace-uri": "", "localname": prop, "collation": STRING_COLLATION}


def _path_index(path: str) -> dict:
    return {
        "scalar-type": "string",
        "path-expression": path,
        "collation": STRING_COLLATION,
        "range-value-positions": False,
        "invalid-values": "ignore",
    }


def _excluded_path(path: str) -> dict:
    return {"excluded-path-expression": path}


async def _ensure_forests(
    client: AdminClient, database: str, partitions: int, replication: bool
) -> None:
    # Databases provisioned before partitioning keep their single forest
    if await client.get_forest(f"{database}-forest") is not None:
        return
    hosts = sorted(await client.list_hosts())
    if not hosts:
        raise RuntimeError("MarkLogic returned no hosts for forest creation")
    if replication and len(hosts) < 2:
        logger.warning(
            "Forest replication requires at least two hosts, creating forests without replicas",
            extra={"database": database},
        )
        replication = False
    # Stable hash so forests are spread evenly across the cluster instead of piling on one host
    offset = zlib.crc32(database.encode())
    for partition in range(partitions):
        forest_name = f"{database}-forest-{partition}"
        if await client.get_forest(forest_name) is not None:
            continue
        host = hosts[(offset + partition) % len(hosts)]
        replica = None
        if replication:
            replica_host = hosts[(offset + partition + 1) % len(hosts)]
            replica = (f"{forest_name}-replica", replica_host)
        await client.create_forest(forest_name, host=host, database=database, replica=replica)


async def _reconcile_properties(
    client: AdminClient,
    database: str,
    path_indexes: Sequence[str],
    word_lexicon_properties: Sequence[str],
    excluded_paths: Sequence[str],
) -> None:
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
    existing_lexicons = properties.get("element-word-lexicon", [])
    missing_lexicons = [
        prop
        for prop in word_lexicon_properties
        if not any(item.get("localname") == prop for item in existing_lexicons)
    ]
    if missing_lexicons:
        changed["element-word-lexicon"] = [
            *existing_lexicons,
            *(_word_lexicon(prop) for prop in missing_lexicons),
        ]
    existing_excluded = properties.get("excluded-path", [])
    missing_excluded = [
        path
        for path in excluded_paths
        if not any(item.get("excluded-path-expression") == path for item in existing_excluded)
    ]
    if missing_excluded:
        changed["excluded-path"] = [
            *existing_excluded,
            *(_excluded_path(path) for path in missing_excluded),
        ]
    if not changed:
        return
    await client.update_database_properties(database, changed)


async def ensure_database(
    client: AdminClient,
    database: str,
    path_indexes: Sequence[str],
    word_lexicon_properties: Sequence[str] = (),
    excluded_paths: Sequence[str] = (),
    *,
    partitions: int,
    replication: bool,
) -> None:
    """Create the database with its forests and indexes, or reconcile an existing one."""
    if await client.get_database(database) is None:
        await client.create_database(
            {
                "database-name": database,
                "schema-database": SCHEMAS_DATABASE,
                "triggers-database": "Triggers",
                **DATABASE_PROPERTIES,
                "range-path-index": [_path_index(path) for path in path_indexes],
                "element-word-lexicon": [_word_lexicon(prop) for prop in word_lexicon_properties],
                "excluded-path": [_excluded_path(path) for path in excluded_paths],
            }
        )
    else:
        await _reconcile_properties(
            client, database, path_indexes, word_lexicon_properties, excluded_paths
        )
    await _ensure_forests(client, database, partitions, replication)
