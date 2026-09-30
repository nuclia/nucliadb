"""Create and configure the shared MarkLogic content database."""

from __future__ import annotations

import httpx

from nucliadb.common.maindb.index_paths import MarkLogicIndexPaths
from nucliadb.common.maindb.marklogic import MarkLogicDriver

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
RANGE_PATH_INDEXES = (
    MarkLogicIndexPaths.MAINDB_KEY,
    MarkLogicIndexPaths.KBID,
    MarkLogicIndexPaths.SHARD,
    MarkLogicIndexPaths.SLUG,
    MarkLogicIndexPaths.TITLE,
)


async def _request(client: httpx.AsyncClient, method: str, path: str, **kwargs) -> httpx.Response:
    response = await client.request(method, path, **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(f"MarkLogic migration request failed: {response.status_code} {response.text}")
    return response


async def _ensure_database(client: httpx.AsyncClient, database: str) -> None:
    response = await client.get(f"/manage/v2/databases/{database}", params={"format": "json"})
    if response.status_code == 200:
        return
    if response.status_code != 404:
        raise RuntimeError(f"Cannot inspect MarkLogic database: {response.status_code} {response.text}")
    await _request(
        client,
        "POST",
        "/manage/v2/databases",
        params={"format": "json"},
        json={
            "database-name": database,
            "schema-database": "Schemas",
            "triggers-database": "Triggers",
            **DATABASE_PROPERTIES,
        },
    )


async def _ensure_forest(client: httpx.AsyncClient, database: str) -> None:
    response = await _request(client, "GET", "/manage/v2/forests", params={"format": "json"})
    forests = response.json().get("forest-default-list", {}).get("list-items", {}).get("list-item", [])
    forest_name = f"{database}-forest"
    if any((forest.get("nameref") or forest.get("forest-name")) == forest_name for forest in forests):
        return
    hosts_response = await _request(client, "GET", "/manage/v2/hosts", params={"format": "json"})
    hosts = hosts_response.json().get("host-default-list", {}).get("list-items", {}).get("list-item", [])
    if not hosts:
        raise RuntimeError("MarkLogic returned no hosts for forest creation")
    host = hosts[0].get("nameref") or hosts[0].get("id")
    await _request(
        client,
        "POST",
        "/manage/v2/forests",
        params={"format": "json", "wait-for-forest-to-mount": "true"},
        json={"forest-name": forest_name, "host": host, "database": database},
    )


async def _ensure_indexes(client: httpx.AsyncClient, database: str) -> None:
    index_response = await _request(
        client,
        "GET",
        f"/manage/v2/databases/{database}/properties",
        headers={"Accept": "application/json"},
    )
    properties = index_response.json()
    path_indexes = properties.get("range-path-index", [])
    for path in RANGE_PATH_INDEXES:
        if not any(item.get("path-expression") == path for item in path_indexes):
            path_indexes.append(
                {
                    "scalar-type": "string",
                    "path-expression": path,
                    "collation": STRING_COLLATION,
                    "range-value-positions": False,
                    "invalid-values": "ignore",
                }
            )
    await _request(
        client,
        "PUT",
        f"/manage/v2/databases/{database}/properties",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={"range-path-index": path_indexes},
    )


async def migrate(driver: MarkLogicDriver) -> None:
    async with httpx.AsyncClient(
        base_url=f"{driver.uri}:{driver.admin_port}",
        auth=httpx.DigestAuth(driver.username, driver.password),
        timeout=60,
    ) as client:
        await _ensure_database(client, driver.database)
        await _ensure_forest(client, driver.database)
        properties_response = await _request(
            client,
            "GET",
            f"/manage/v2/databases/{driver.database}/properties",
            headers={"Accept": "application/json"},
        )
        properties = properties_response.json()
        changed = {key: value for key, value in DATABASE_PROPERTIES.items() if not properties.get(key)}
        if changed:
            await _request(
                client,
                "PUT",
                f"/manage/v2/databases/{driver.database}/properties",
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                json=changed,
            )
        await _ensure_indexes(client, driver.database)
