"""Native asyncio MarkLogic REST client built on httpx.

Implements the subset of the marklogic-python-client API used by the maindb driver and datamanagers.
"""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from decimal import Decimal
from email.message import Message
from typing import Any

import httpx

DEFAULT_TIMEOUT = 60.0


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


def _parse_multipart(response: httpx.Response) -> list[_Part]:
    message = Message()
    message["content-type"] = response.headers.get("content-type", "")
    boundary = message.get_param("boundary")
    if not isinstance(boundary, str):
        raise ValueError("Multipart response without boundary")
    parts = []
    for chunk in response.content.split(b"--" + boundary.encode())[1:]:
        if chunk.startswith(b"--"):
            break
        raw_headers, _, body = chunk.removeprefix(b"\r\n").partition(b"\r\n\r\n")
        headers = {}
        for line in raw_headers.decode("utf-8").split("\r\n"):
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
        parts.append(_Part(headers, body.removesuffix(b"\r\n")))
    return parts


def _part_disposition(part: _Part) -> tuple[str | None, str | None]:
    disposition = part.headers.get("content-disposition", "")
    message = Message()
    message["content-disposition"] = disposition
    uri = message.get_filename()
    if uri is None:
        return None, None
    # URIs are not always quoted, so strip it before splitting the remaining parameters
    category = None
    for item in disposition.replace(uri, "").split(";"):
        key, _, value = item.partition("=")
        if key.strip() == "category":
            category = value.strip()
    return uri, category


def _parse_documents(response: httpx.Response) -> list[Document]:
    documents = []
    for part in _parse_multipart(response):
        uri, category = _part_disposition(part)
        if uri is None or category != "content":
            continue
        content_type = part.headers.get("content-type")
        content: Any = part.content
        if content_type == "application/json":
            content = json.loads(part.content)
        elif content_type in ("application/xml", "text/xml", "text/plain"):
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
    def __init__(self, id: str, http: httpx.AsyncClient):
        self.id = id
        self._http = http

    async def commit(self) -> httpx.Response:
        return await self._http.post(f"/v1/transactions/{self.id}", params={"result": "commit"})

    async def rollback(self) -> httpx.Response:
        return await self._http.post(f"/v1/transactions/{self.id}", params={"result": "rollback"})


class TransactionManager:
    def __init__(self, http: httpx.AsyncClient):
        self._http = http

    async def create(self, database: str | None = None) -> Transaction:
        params = {"database": database} if database else {}
        response = await self._http.post(
            "/v1/transactions", params=params, headers={"Accept": "application/json"}
        )
        if response.status_code == 303 and "location" in response.headers:
            return Transaction(response.headers["location"].rstrip("/").rsplit("/", 1)[-1], self._http)
        if not response.is_success:
            raise RuntimeError(f"Failed to create transaction: {response.status_code} {response.text}")
        return Transaction(response.json()["transaction-status"]["transaction-id"], self._http)


class DocumentManager:
    def __init__(self, http: httpx.AsyncClient):
        self._http = http

    async def read(
        self,
        uris: str | list[str],
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[Document] | httpx.Response:
        """Returns the documents found, or the raw response if MarkLogic did not answer 200."""
        params = _with_txid(params, tx)
        params["uri"] = uris if isinstance(uris, list) else [uris]
        params["format"] = "json"
        response = await self._http.get(
            "/v1/documents", params=params, headers={"Accept": "multipart/mixed"}
        )
        if response.status_code != 200:
            return response
        return _parse_documents(response)

    async def write(
        self,
        documents: Document | list[Document],
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        if isinstance(documents, Document):
            documents = [documents]
        data, content_type = _encode_multipart(documents)
        return await self._http.post(
            "/v1/documents",
            content=data,
            params=_with_txid(params, tx),
            headers={"Content-Type": content_type, "Accept": "application/json"},
        )

    async def delete(
        self,
        uris: str | list[str],
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        params = _with_txid(params, tx)
        params["uri"] = uris if isinstance(uris, list) else [uris]
        return await self._http.delete("/v1/documents", params=params)


class RowManager:
    def __init__(self, http: httpx.AsyncClient):
        self._http = http

    async def update(
        self,
        dsl: str,
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        return await self._http.post(
            "/v1/rows/update",
            content=dsl.encode("utf-8"),
            params=_with_txid(params, tx),
            headers={
                "Content-Type": "application/vnd.marklogic.querydsl+javascript",
                "Accept": "application/json",
            },
        )


class Client:
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
        self.documents = DocumentManager(self._http)
        self.rows = RowManager(self._http)
        self.transactions = TransactionManager(self._http)

    async def eval(
        self,
        javascript: str,
        vars: dict[str, Any] | None = None,
        tx: Transaction | None = None,
        params: dict[str, Any] | None = None,
    ) -> list[Any] | httpx.Response | None:
        """Returns the evaluated values, None when there are none, or the raw response on error."""
        data = {"javascript": javascript}
        if vars:
            data["vars"] = json.dumps(vars)
        response = await self._http.post("/v1/eval", data=data, params=_with_txid(params, tx))
        if response.status_code != 200:
            return response
        if _has_no_content(response):
            return None
        return [_parse_eval_part(part) for part in _parse_multipart(response)]

    async def aclose(self) -> None:
        await self._http.aclose()
