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
"""Native asyncio MarkLogic REST client built on httpx.

Implements the subset of the marklogic-python-client API used by the maindb driver and datamanagers.
"""

from __future__ import annotations

import json
import logging
import secrets
from dataclasses import dataclass
from decimal import Decimal
from email.message import Message
from email.parser import BytesParser
from email.policy import default
from typing import Any

import backoff
import httpx

from nucliadb.common.marklogic.exceptions import (
    DatabaseDoesNotExist,
    MarkLogicProtocolError,
    MarkLogicResponseError,
)

DEFAULT_TIMEOUT = 60.0

# 503 "Retryable Error" codes MarkLogic returns while a new database is still coming online
RETRYABLE_ERROR_CODES = (b"XDMP-DBTIMESTAMP", b"XDMP-FORESTNOTOPEN")
RETRY_ATTEMPTS = 10
RETRY_MAX_DELAY = 2.0

logger = logging.getLogger(__name__)


def _is_database_not_ready(response: httpx.Response) -> bool:
    # Requests are sent non-streamed, so the body is already loaded
    return response.status_code == 503 and any(
        code in response.content for code in RETRYABLE_ERROR_CODES
    )


def _log_retry(details: Any) -> None:
    logger.warning(
        "MarkLogic database not ready, retrying request",
        extra={"url": str(details["args"][1].url), "tries": details["tries"]},
    )


class _RetryingAsyncClient(httpx.AsyncClient):
    """Retries requests rejected before execution because a database is not ready yet."""

    @backoff.on_predicate(
        backoff.expo,
        _is_database_not_ready,
        factor=0.1,
        max_value=RETRY_MAX_DELAY,
        max_tries=RETRY_ATTEMPTS,
        on_backoff=_log_retry,
    )
    async def send(self, request: httpx.Request, **kwargs: Any) -> httpx.Response:
        return await super().send(request, **kwargs)


@dataclass
class Document:
    uri: str
    content: Any = None
    collections: list[str] | None = None
    content_type: str | None = None


@dataclass
class _Part:
    headers: dict[str, str]
    content: bytes

    @property
    def text(self) -> str:
        return self.content.decode("utf-8")


def _with_txid(params: dict[str, Any] | None, tx: Transaction | None) -> dict[str, Any]:
    params = dict(params or {})
    if tx is not None:
        params["txid"] = tx.id
    return params


def _has_no_content(response: httpx.Response) -> bool:
    return response.headers.get("content-length") == "0" or not response.content


def _check(response: httpx.Response, operation: str) -> None:
    if response.is_success:
        return
    code = None
    try:
        error = response.json().get("errorResponse", {})
        code = error.get("messageCode")
    except (ValueError, AttributeError, TypeError):
        pass
    error_type = MarkLogicResponseError
    if response.status_code in (400, 404) and (
        code == "XDMP-NOSUCHDB" or "No such database" in response.text
    ):
        error_type = DatabaseDoesNotExist
    raise error_type(
        f"Failed to {operation}: {response.status_code} {response.text}", response=response, code=code
    )


def _parse_multipart(response: httpx.Response) -> list[_Part]:
    content_type = response.headers.get("content-type", "")
    message = BytesParser(policy=default).parsebytes(
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode() + response.content
    )
    if not message.is_multipart() or message.defects:
        raise MarkLogicProtocolError("Invalid multipart response")
    parts: list[_Part] = []
    for part in message.iter_parts():
        content = part.get_payload(decode=True)
        if part.defects or not isinstance(content, bytes):
            raise MarkLogicProtocolError("Invalid multipart part")
        parts.append(_Part({name.lower(): str(value) for name, value in part.items()}, content))
    return parts


def _part_disposition(part: _Part) -> tuple[str | None, str | None]:
    disposition = part.headers.get("content-disposition", "")
    message = Message()
    message["content-disposition"] = disposition
    uri = message.get_filename()
    if uri is None:
        return None, None
    category = message.get_param("category", header="content-disposition")
    return uri, category if isinstance(category, str) else None


def _parse_documents(response: httpx.Response) -> list[Document]:
    documents = []
    for part in _parse_multipart(response):
        uri, category = _part_disposition(part)
        if category == "metadata":
            continue
        if uri is None or category != "content":
            raise MarkLogicProtocolError("Document part without URI or content category")
        content_type = part.headers.get("content-type")
        message = Message()
        message["content-type"] = content_type or "application/octet-stream"
        media_type = message.get_content_type()
        content: Any = part.content
        if media_type == "application/json":
            content = json.loads(part.content)
        elif media_type in ("application/xml", "text/xml", "text/plain"):
            content = part.text
        documents.append(Document(uri=uri, content=content, content_type=content_type))
    return documents


def _parse_eval_part(part: _Part) -> Any:
    primitive = part.headers.get("x-primitive")
    if primitive == "integer":
        return int(part.text)
    if primitive == "decimal":
        return Decimal(part.text)
    if primitive == "boolean":
        return part.text == "true"
    if primitive in ("map", "array", "array-node()", "object-node()"):
        return json.loads(part.text)
    if primitive in ("string", "element()", "document-node()"):
        return part.text
    return part.content


def _quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _serialize_content(content: Any) -> bytes:
    if isinstance(content, bytes):
        return content
    if isinstance(content, str):
        return content.encode("utf-8")
    return json.dumps(content).encode("utf-8")


def _encode_multipart(documents: list[Document]) -> tuple[bytes, str]:
    boundary = secrets.token_hex(16)
    body = bytearray()

    def add_part(disposition: str, content_type: str | None, data: bytes) -> None:
        body.extend(f"--{boundary}\r\nContent-Disposition: {disposition}\r\n".encode())
        if content_type:
            body.extend(f"Content-Type: {content_type}\r\n".encode())
        body.extend(b"\r\n")
        body.extend(data)
        body.extend(b"\r\n")

    for document in documents:
        filename = _quote(document.uri)
        if document.collections:
            add_part(
                f'attachment; filename="{filename}"; category=metadata',
                "application/json",
                json.dumps({"collections": document.collections}).encode("utf-8"),
            )
        if document.content is not None:
            add_part(
                f'attachment; filename="{filename}"',
                document.content_type,
                _serialize_content(document.content),
            )
    body.extend(f"--{boundary}--\r\n".encode())
    return bytes(body), f"multipart/mixed; boundary={boundary}"


class Transaction:
    def __init__(self, id: str, http: httpx.AsyncClient, database: str | None = None):
        """
        Initialize a Transaction instance.

        Args:
            id: The transaction ID.
            http: The HTTP client to use for requests.
            database: The database associated with the transaction, if any. Otherwise, the default database is used by the server.
        """
        self.id = id
        self._http = http
        self.database = database

    async def _finish(self, result: str) -> None:
        params = {"result": result}
        if self.database is not None:
            params["database"] = self.database
        response = await self._http.post(f"/v1/transactions/{self.id}", params=params)
        _check(response, f"{result} transaction")

    async def commit(self) -> None:
        await self._finish("commit")

    async def rollback(self) -> None:
        await self._finish("rollback")


class TransactionManager:
    def __init__(self, http: httpx.AsyncClient):
        self._http = http

    async def create(self, database: str | None = None) -> Transaction:
        params = {"database": database} if database else {}
        response = await self._http.post(
            "/v1/transactions", params=params, headers={"Accept": "application/json"}
        )
        if response.status_code == 303 and "location" in response.headers:
            return Transaction(
                response.headers["location"].rstrip("/").rsplit("/", 1)[-1], self._http, database
            )
        _check(response, "create transaction")
        try:
            transaction_id = response.json()["transaction-status"]["transaction-id"]
        except (ValueError, KeyError, TypeError) as error:
            raise MarkLogicProtocolError("Transaction response without transaction ID") from error
        return Transaction(str(transaction_id), self._http, database)


class DocumentManager:
    def __init__(self, http: httpx.AsyncClient):
        self._http = http

    async def read(
        self,
        uris: str | list[str],
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[Document]:
        """Return found documents; missing URIs are omitted from the result."""
        if not uris:
            return []
        params = _with_txid(params, tx)
        params["uri"] = uris if isinstance(uris, list) else [uris]
        params["format"] = "json"
        response = await self._http.get(
            "/v1/documents", params=params, headers={"Accept": "multipart/mixed"}
        )
        if response.status_code == 404:
            if "No such database" in response.text or "XDMP-NOSUCHDB" in response.text:
                _check(response, "read documents")
            return []
        _check(response, "read documents")
        if _has_no_content(response):
            return []
        return _parse_documents(response)

    async def write(
        self,
        documents: Document | list[Document],
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> None:
        if isinstance(documents, Document):
            documents = [documents]
        if not documents:
            return
        data, content_type = _encode_multipart(documents)
        response = await self._http.post(
            "/v1/documents",
            content=data,
            params=_with_txid(params, tx),
            headers={"Content-Type": content_type, "Accept": "application/json"},
        )
        _check(response, "write documents")

    async def delete(
        self,
        uris: str | list[str],
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> None:
        if not uris:
            return
        params = _with_txid(params, tx)
        params["uri"] = uris if isinstance(uris, list) else [uris]
        response = await self._http.delete("/v1/documents", params=params)
        _check(response, "delete documents")

    async def exists(
        self,
        uri: str,
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> bool:
        params = _with_txid(params, tx)
        params["uri"] = [uri]
        response = await self._http.head("/v1/documents", params=params)
        if not response.is_success:
            response = await self._http.get(
                "/v1/documents", params=params, headers={"Accept": "application/json"}
            )
        if response.status_code == 404:
            if "No such database" in response.text or "XDMP-NOSUCHDB" in response.text:
                _check(response, "check document existence")
            return False
        _check(response, "check document existence")
        return True


class RowManager:
    def __init__(self, http: httpx.AsyncClient):
        self._http = http

    async def update(
        self,
        dsl: str,
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Return the rows produced by the plan, e.g. one per document written."""
        response = await self._http.post(
            "/v1/rows/update",
            content=dsl.encode("utf-8"),
            params=_with_txid(params, tx),
            headers={
                "Content-Type": "application/vnd.marklogic.querydsl+javascript",
                "Accept": "application/json",
            },
        )
        _check(response, "update rows")
        if _has_no_content(response):
            return []
        try:
            return response.json().get("rows", [])
        except (ValueError, AttributeError) as error:
            raise MarkLogicProtocolError("Invalid rows update response") from error


class Client:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: float | None = DEFAULT_TIMEOUT,
    ):
        self._http = _RetryingAsyncClient(
            base_url=base_url,
            auth=httpx.DigestAuth(username, password),
            timeout=timeout,
        )
        self.documents = DocumentManager(self._http)
        self.rows = RowManager(self._http)
        self.transactions = TransactionManager(self._http)

    async def eval(
        self,
        javascript: str,
        vars: dict[str, Any] | None = None,
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[Any]:
        """Return evaluated values, or an empty list for an empty sequence."""
        data = {"javascript": javascript}
        if vars:
            data["vars"] = json.dumps(vars)
        response = await self._http.post("/v1/eval", data=data, params=_with_txid(params, tx))
        _check(response, "evaluate query")
        if _has_no_content(response):
            return []
        return [_parse_eval_part(part) for part in _parse_multipart(response)]

    async def aclose(self) -> None:
        await self._http.aclose()
