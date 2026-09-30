import logging
from urllib.parse import quote

from .exceptions import MarkLogicError
from .utils import system_request

logger = logging.getLogger(__name__)


def _options_path(name: str) -> str:
    return f"/v1/config/query/{quote(name, safe='')}"


async def put_search_options(server_id: str, name: str, options: dict) -> None:
    """Store named search options on the MarkLogic REST API server (port 8003)."""
    logger.info("Storing search options", extra={"server_id": server_id, "options_name": name})
    resp = await system_request(
        server_id,
        "PUT",
        _options_path(name),
        headers={"Content-Type": "application/json"},
        json=options,
    )
    if resp.status_code not in (200, 201, 204):
        raise MarkLogicError(f"Failed to store search options '{name}': {resp.status_code} {resp.text}")


async def get_search_options(server_id: str, name: str) -> dict | None:
    """Fetch named search options from the MarkLogic REST API server.

    Returns the options dict, or None if no options exist with that name.
    """
    resp = await system_request(
        server_id, "GET", _options_path(name), headers={"Accept": "application/json"}
    )
    if resp.status_code == 404:
        return None
    if resp.status_code not in (200, 201):
        raise MarkLogicError(f"Failed to get search options '{name}': {resp.status_code} {resp.text}")
    return resp.json()


async def list_search_options(server_id: str) -> list[str]:
    """List the names of all stored search options on the MarkLogic REST API server."""
    resp = await system_request(
        server_id, "GET", "/v1/config/query", headers={"Accept": "application/json"}
    )
    if resp.status_code not in (200, 201):
        raise MarkLogicError(f"Failed to list search options: {resp.status_code} {resp.text}")
    # Response shape: [{"name": "...", "uri": "..."}, ...]
    return [item["name"] for item in resp.json() if "name" in item]


async def delete_search_options(server_id: str, name: str) -> None:
    """Delete named search options from the MarkLogic REST API server."""
    logger.info("Deleting search options", extra={"server_id": server_id, "options_name": name})
    resp = await system_request(server_id, "DELETE", _options_path(name))
    if resp.status_code not in (200, 204, 404):
        raise MarkLogicError(f"Failed to delete search options '{name}': {resp.status_code} {resp.text}")
