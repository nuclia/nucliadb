"""Native asyncio client for the MarkLogic Management REST API built on httpx.

Implements the subset of the /manage/v2 endpoints used to provision maindb databases.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any
from urllib.parse import quote

import httpx

DEFAULT_TIMEOUT = 60.0
_JSON = {"format": "json"}


def _check(response: httpx.Response, operation: str) -> None:
    if not response.is_success:
        raise RuntimeError(f"Failed to {operation}: {response.status_code} {response.text}")


def _list_items(payload: dict[str, Any], root: str) -> list[dict[str, Any]]:
    return payload.get(root, {}).get("list-items", {}).get("list-item", [])


class AdminClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: float | None = DEFAULT_TIMEOUT,
    ):
        self._http = httpx.AsyncClient(
            base_url=base_url,
            auth=httpx.DigestAuth(username, password),
            timeout=timeout,
        )

    async def __aenter__(self) -> AdminClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def list_databases(self) -> list[str]:
        response = await self._http.get("/manage/v2/databases", params=_JSON)
        _check(response, "list databases")
        return [str(item["nameref"]) for item in _list_items(response.json(), "database-default-list")]

    async def get_database(self, database: str) -> dict[str, Any] | None:
        response = await self._http.get(f"/manage/v2/databases/{quote(database, safe='')}", params=_JSON)
        if response.status_code == 404:
            return None
        _check(response, "inspect database")
        return response.json()

    async def create_database(self, properties: dict[str, Any]) -> None:
        response = await self._http.post("/manage/v2/databases", params=_JSON, json=properties)
        _check(response, "create database")

    async def get_database_properties(self, database: str) -> dict[str, Any]:
        response = await self._http.get(
            f"/manage/v2/databases/{quote(database, safe='')}/properties",
            headers={"Accept": "application/json"},
        )
        _check(response, "read database properties")
        return response.json()

    async def update_database_properties(self, database: str, properties: dict[str, Any]) -> None:
        response = await self._http.put(
            f"/manage/v2/databases/{quote(database, safe='')}/properties",
            headers={"Accept": "application/json"},
            json=properties,
        )
        _check(response, "update database properties")

    async def delete_database(self, database: str) -> None:
        """Drop the database and its forests. Missing databases are ignored."""
        response = await self._http.delete(
            f"/manage/v2/databases/{quote(database, safe='')}",
            params={**_JSON, "forest-delete": "data"},
        )
        if response.status_code == 404:
            return
        _check(response, "delete database")

    async def get_forest(self, forest: str) -> dict[str, Any] | None:
        response = await self._http.get(f"/manage/v2/forests/{quote(forest, safe='')}", params=_JSON)
        if response.status_code == 404:
            return None
        _check(response, "inspect forest")
        return response.json()

    async def create_forest(self, forest: str, host: str, database: str) -> None:
        response = await self._http.post(
            "/manage/v2/forests",
            params={**_JSON, "wait-for-forest-to-mount": "true"},
            json={"forest-name": forest, "host": host, "database": database},
        )
        _check(response, "create forest")

    async def list_hosts(self) -> list[str]:
        response = await self._http.get("/manage/v2/hosts", params=_JSON)
        _check(response, "list hosts")
        return [
            str(item.get("nameref") or item.get("id"))
            for item in _list_items(response.json(), "host-default-list")
        ]
