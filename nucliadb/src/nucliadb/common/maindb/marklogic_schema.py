"""MarkLogic database provisioning shared by schema migrations and KnowledgeBox creation."""

from __future__ import annotations

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
    hosts = await client.list_hosts()
    if not hosts:
        raise RuntimeError("MarkLogic returned no hosts for forest creation")
    await client.create_forest(forest_name, host=hosts[0], database=database)


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
