import asyncio
import json
import logging
import tempfile
import time
import uuid
from collections.abc import AsyncIterator
from functools import partial
from typing import Any, AsyncGenerator

import backoff
import httpx
import requests
from lru import LRU
from marklogic.documents import Document, multipart_response_to_documents  # type: ignore[import-untyped]
from yarl import URL

from . import (
    const,  # noqa
    indexes,  # noqa
    tde,  # noqa
)
from .exceptions import DatabaseDoesNotExist as DatabaseDoesNotExist
from .exceptions import MarkLogicError
from .exceptions import NoHealthyUpstreamError as NoHealthyUpstreamError
from .exceptions import RoleDoesNotExist as RoleDoesNotExist
from .models import (
    MarkLogicDatabaseLocator,
    MraRetrieveDefinitionResponse,
    MraRetrieveRequest,
    MraRetrieveResponse,
)
from .settings import META_MARKLOGIC_SERVER_ID, env_settings
from .utils import (
    check_marklogic_response,
    close,  # noqa
    get_client_for_context,
    get_httpx_system_client_for_context,
    get_marklogic_executor,
    request,
    retry_on_error,
    system_request,  # noqa
)
from .utils import (
    get_admin_marklogic_client as get_admin_marklogic_client,
)
from .utils import (
    get_user_marklogic_client as get_user_marklogic_client,
)
from .utils import (
    set_marklogic_account_id as set_marklogic_account_id,
)
from .utils import (
    set_marklogic_user_id as set_marklogic_user_id,
)
from .utils import (
    with_admin_client as with_admin_client,
)
from .utils import (
    with_admin_client_handler as with_admin_client_handler,
)
from .utils import (
    with_user as with_user,
)

logger = logging.getLogger(__name__)


class _ReplayableAsyncFileStream(httpx.AsyncByteStream):
    def __init__(self, file: tempfile.SpooledTemporaryFile[bytes]) -> None:
        self.file = file

    async def __aiter__(self) -> AsyncIterator[bytes]:
        await asyncio.to_thread(self.file.seek, 0)
        while chunk := await asyncio.to_thread(self.file.read, 1024 * 1024):
            yield chunk


async def wait_for_ready(server_id: str, max_time: float = 60) -> None:  # pragma: no cover
    server_settings = env_settings.get_marklogic_server(server_id)
    base_url = URL(server_settings.uri).with_port(server_settings.init_port)
    start = time.time()
    async with httpx.AsyncClient() as client:
        while time.time() - start < max_time:
            try:
                resp = await client.get(base_url.with_path("/admin/v1/timestamp").human_repr())
                if resp.status_code in (200, 401, 403):  # "server is answering"
                    break
            except (Exception, httpx.ReadError):
                logger.warning("MarkLogic not ready yet, retrying...")
            await asyncio.sleep(1)


@backoff.on_exception(
    backoff.expo,
    (Exception,),
    max_time=300,
    jitter=backoff.full_jitter,
    max_tries=10,
    on_backoff=lambda details: logger.warning(
        "MarkLogic initialization failed, retrying",
        extra={
            "wait": details["wait"],
            "tries": details["tries"],
            "exception": str(details["exception"]),
        },
    ),
)
async def initialize_marklogic():  # pragma: no cover
    async with httpx.AsyncClient() as client:
        server_settings = env_settings.get_marklogic_server(META_MARKLOGIC_SERVER_ID)
        base_url = URL(server_settings.uri).with_port(server_settings.init_port)

        await wait_for_ready(META_MARKLOGIC_SERVER_ID)

        resp = await client.post(
            base_url.with_path("/admin/v1/init").human_repr(),
            content=b"",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Content-Length": "0",
            },
        )
        if resp.status_code == 401:
            # already initialized
            return

        if resp.status_code not in (202, 204):
            raise MarkLogicError(f"Failed to initialize MarkLogic: {resp.status_code} {resp.text}")

        await wait_for_ready(META_MARKLOGIC_SERVER_ID)
        assert server_settings.username is not None, (
            f"username is required for MarkLogic server {META_MARKLOGIC_SERVER_ID}"
        )
        assert server_settings.password is not None, (
            f"password is required for MarkLogic server {META_MARKLOGIC_SERVER_ID}"
        )

        resp = await client.post(
            base_url.with_path("/admin/v1/instance-admin").human_repr(),
            timeout=60,
            json={
                "admin-username": server_settings.username,
                "admin-password": server_settings.password.get_secret_value(),
                "wallet-password": server_settings.password.get_secret_value(),
                "realm": "public",
            },
            params={"format": "json"},
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        if resp.status_code not in (202, 401):
            raise MarkLogicError(
                f"Failed to initialize MarkLogic security: {resp.status_code} {resp.text}"
            )
        else:
            # wait for it to be ready
            await wait_for_ready(META_MARKLOGIC_SERVER_ID)
            await asyncio.sleep(3)  # extra wait to ensure it's ready


@retry_on_error
async def write(db: MarkLogicDatabaseLocator, document: Document | list[Document]) -> None:
    """Write one or many documents in a single request.

    Pass a ``list[Document]`` to bulk-write multiple documents bound for the
    same database in one round trip, rather than issuing a separate ``write``
    call per document.
    """
    client = get_client_for_context(db.server_id)
    resp = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(client.documents.write, document, params={"database": db.database}),
    )
    if not resp.ok:
        check_marklogic_response(resp, "write document")


@retry_on_error
async def admin_write(db: MarkLogicDatabaseLocator, document: Document | list[Document]) -> None:
    """Write a document using the admin client, bypassing any per-user context.

    Use this for system-internal documents (e.g. locks) that should always be
    written with admin credentials regardless of the current request context.
    """
    client = get_admin_marklogic_client(db.server_id)
    resp = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(client.documents.write, document, params={"database": db.database}),
    )
    if not resp.ok:  # pragma: no cover
        check_marklogic_response(resp, "write document")


@retry_on_error
async def admin_get(
    db: MarkLogicDatabaseLocator,
    uri: str,
    categories: list[str] | None = None,
) -> Document | None:
    """Read a document using the admin client, bypassing any per-user context."""
    client = get_admin_marklogic_client(db.server_id)
    res = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(client.documents.read, [uri], categories=categories, params={"database": db.database}),
    )
    if not isinstance(res, list) and isinstance(res, requests.Response):  # pragma: no cover
        if res.status_code == 400 and "No such database" in res.text:
            return None
        check_marklogic_response(res, "read document")
    uri_to_doc = {doc.uri: doc for doc in res}
    return uri_to_doc.get(uri)


_DELETE_BATCH_SIZE = 500


def _delete_uris_optic(uris: list[str]) -> str:
    """Build an Optic Update DSL to delete documents by URI.

    Uses op.fromLiterals with JSON-serialized URI values — no query needed,
    and json.dumps ensures safe encoding of any URI characters.
    """
    literals = json.dumps([{"uri": uri} for uri in uris])
    return f"op.fromLiterals({literals}).remove()"


async def get(
    db: MarkLogicDatabaseLocator,
    uri: str,
    categories: list[str] | None = None,
) -> Document | None:
    res = await get_all(db, [uri], categories=categories)
    if len(res) == 0:
        return None
    return res[0]


@retry_on_error
async def get_all(
    db: MarkLogicDatabaseLocator,
    uris: list[str],
    categories: list[str] | None = None,
) -> list[Document]:
    if len(uris) == 0:
        return []
    client = get_client_for_context(db.server_id)

    # See https://marklogic.github.io/marklogic-python-client/documents/searching#searching-via-a-complex-query
    # Using a POST to v1/search should avoid any issues with querystring being too large.
    serialized_cts_query = {"ctsquery": {"documentQuery": {"uris": uris}}}

    res = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(
            client.documents.search,
            query=serialized_cts_query,
            categories=categories,
            params={"database": db.database},
            page_length=len(uris),
        ),
    )

    if not isinstance(res, list) and isinstance(res, requests.Response):
        if res.status_code == 400 and "No such database" in res.text:
            # Treat "No such database" as "not found" for all URIs
            return []
        check_marklogic_response(res, "read documents")

    # force order by input uris
    uri_to_doc = {doc.uri: doc for doc in res}
    return [uri_to_doc[uri] for uri in uris if uri in uri_to_doc]


async def delete(db: MarkLogicDatabaseLocator, uris: list[str] | str) -> None:
    if isinstance(uris, str):
        uris = [uris]
    for i in range(0, len(uris), _DELETE_BATCH_SIZE):
        await rows_update(db, _delete_uris_optic(uris[i : i + _DELETE_BATCH_SIZE]))


def _patch_document_optic(
    uri: str, values: dict[str, Any], namespaces: dict[str, str] | None = None
) -> str:
    """Build an Optic Update DSL that patches a single document's top-level properties in place.

    Uses `op.patchBuilder(...).replaceValue(name, value)` chained once per entry in `values`, so
    only the named properties are rewritten server-side rather than the whole document. This only
    works for properties that already exist in the document (with any value, including null) --
    `replaceValue` is a no-op (not an insert) for a genuinely absent property.

    For XML documents, a bare property name (e.g. `"status"`) won't resolve a namespaced element
    (e.g. `<pdp:status>`) -- pass `namespaces` (prefix -> URI) and prefix each name in `values`
    accordingly (e.g. `"pdp:status"`) so `replaceValue` can resolve it; `op.patchBuilder` accepts
    the namespace bindings as a second argument.

    `uri`, each property name/value, and the namespaces map are JSON-encoded directly into the
    query text (there's no op.param support for patchBuilder arguments in the MarkLogic version in
    use), so they can't break out of their string/value literals.
    """
    patches = "".join(
        f".replaceValue({json.dumps(name)}, {json.dumps(value)})" for name, value in values.items()
    )
    namespaces_arg = f", {json.dumps(namespaces)}" if namespaces else ""
    return (
        f"op.fromLiterals([{{uri: {json.dumps(uri)}}}])"
        f".joinDoc(op.col('doc'), op.col('uri'))"
        f".patch(op.col('doc'), op.patchBuilder('/'{namespaces_arg}){patches})"
        f".write()"
    )


async def patch_document(
    db: MarkLogicDatabaseLocator,
    uri: str,
    values: dict[str, Any],
    namespaces: dict[str, str] | None = None,
) -> bool:
    """
    Patch a single document's top-level properties in place via Optic Update, avoiding a full
    read + rewrite of the document. Only replaces properties that already exist in the document
    (see `_patch_document_optic`); returns False (and patches nothing) if the document doesn't
    exist, so callers can fall back to a full read + rewrite (e.g. to raise a not-found error, or
    to patch a property that isn't already present).

    Pass `namespaces` (and namespace-prefixed keys in `values`) when patching an XML document
    whose properties live in a namespace (see `_patch_document_optic`).
    """
    if not values:
        return True
    result = await rows_update(db, _patch_document_optic(uri, values, namespaces))
    return len(result) > 0


def _delete_by_collection_optic(collections: list[str]) -> str:
    """Build an Optic Update DSL to delete documents in any of `collections`.

    The MarkLogic version in use does not yet support op.param for a
    cts.collectionQuery, so collection names can't be passed as bound
    params. Instead they're JSON-encoded directly into the query text:
    json.dumps produces a valid Optic/JS array-of-strings literal with
    proper escaping, so a collection name can't break out of its string
    literal or inject additional DSL.
    """
    collections_literal = json.dumps(collections)
    return f"op.fromDocUris(cts.collectionQuery({collections_literal})).remove()"


async def delete_by_collection(
    db: MarkLogicDatabaseLocator,
    collection: str | list[str],
) -> None:
    """
    Delete all documents in a collection (or collections) in a single Optic
    Update operation. Suitable for use when the collection(s) are expected to
    be small enough for MarkLogic to delete in a single transaction.
    """
    collections = [collection] if isinstance(collection, str) else collection
    await rows_update(db, _delete_by_collection_optic(collections))


@retry_on_error
async def rows_update(
    db: MarkLogicDatabaseLocator,
    dsl: str,
    params: dict | None = None,
) -> list[dict]:
    """
    Execute an Optic Update plan (e.g. .remove() or .write()) against the
    rows endpoint. Returns the result rows (e.g. aggregates) if any.
    """
    client = get_client_for_context(db.server_id)
    all_params = {**(params or {}), "database": db.database}
    result = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(client.rows.update, dsl=dsl, params=all_params, return_response=True),
    )
    if isinstance(result, requests.Response):
        if not result.ok:
            check_marklogic_response(result, "execute rows update")
        return result.json().get("rows", []) if result.text else []
    return result if isinstance(result, list) else []


@retry_on_error
async def search(
    db: MarkLogicDatabaseLocator,
    *,
    collection: str | None = None,
    start: int = 1,
    limit: int = 100,
    q: str | None = None,
    query: dict | str | None = None,
    categories: list[str] | None = None,
    directory: str | None = None,
    options: str | None = None,
    return_response: bool = False,
) -> list[Document]:
    client = get_client_for_context(db.server_id)

    params = {"database": db.database}
    if directory is not None:
        params["directory"] = directory
    if options is not None:
        params["options"] = options

    result = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(
            client.documents.search,
            collections=[collection] if collection is not None else None,
            categories=categories,
            start=start,
            page_length=limit,
            q=q,
            query=query,
            params=params,
            return_response=return_response,
        ),
    )
    if not return_response and isinstance(result, requests.Response):
        check_marklogic_response(result, "search documents")
    return result


def documents_and_total_from_response(
    response: requests.Response, action_type: str
) -> tuple[int, list[Document]]:
    """Parse the raw multipart response from `search(..., return_response=True)` into the total
    match-count estimate MarkLogic reports alongside the page of documents it contains.

    The total result-estimate is only available as a response header
    (``vnd.marklogic.result-estimate``), so callers that need it alongside search hits must
    request the raw ``requests.Response`` instead of the already-parsed ``list[Document]``. This
    helper centralizes that raw-response -> (total, docs) conversion instead of duplicating it
    across every paged-search repository function.
    """
    if not response.ok:
        check_marklogic_response(response, action_type)
    docs = multipart_response_to_documents(response)
    total = int(response.headers.get("vnd.marklogic.result-estimate", 0))
    return total, docs


@retry_on_error
async def rows(
    db: MarkLogicDatabaseLocator,
    *,
    dsl: str | None = None,
    plan: dict | None = None,
    sql: str | None = None,
    sparql: str | None = None,
    graphql: str | None = None,
    params: dict | None = None,
) -> list[dict]:
    client = get_client_for_context(db.server_id)

    params = {**(params or {}), "database": db.database}
    query_kwargs: dict = {
        "dsl": dsl,
        "plan": plan,
        "sql": sql,
        "sparql": sparql,
        "graphql": graphql,
        "params": params,
    }

    result = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(client.rows.query, **query_kwargs),
    )
    if isinstance(result, requests.Response):
        check_marklogic_response(result, "execute rows query")

    return result or []


@retry_on_error
async def post_sparql(
    db: MarkLogicDatabaseLocator,
    sparql: str,
    *,
    params: dict[str, str | list[str]] | None = None,
    graphs: list[str] | None = None,
) -> dict | None:
    # Convenience for POST'ing a SPARQL query without having to deal with funky
    # headers and converting the response into JSON.
    all_params: dict[str, str | list[str]] = {**(params or {}), "database": db.database}
    if graphs:
        all_params["collection"] = graphs
    response = await request(
        db.server_id,
        "POST",
        "/v1/graphs/sparql",
        params=all_params,
        headers={
            "Content-Type": "application/sparql-query",
            "Accept": "application/sparql-results+json, application/rdf+json",
        },
        data=sparql,
    )

    if response.status_code >= 400:
        raise MarkLogicError(f"SPARQL query failed: {response.status_code} {response.text}")
    if not response.content:
        return None

    try:
        result = response.json()
    except ValueError as exc:
        content_type = response.headers.get("content-type", "")
        raise MarkLogicError(f"SPARQL response was not JSON (content-type={content_type!r})") from exc
    if not isinstance(result, dict):
        raise MarkLogicError("SPARQL response was not a JSON object")
    return result


@retry_on_error
async def describe_thing(db: MarkLogicDatabaseLocator, iri: str) -> dict:
    """Fetch a Concise Bounded Description of ``iri`` -- every triple in the
    database with ``iri`` as its subject -- via MarkLogic's built-in
    ``/v1/graphs/things`` REST endpoint. Unlike ``post_sparql``, this bypasses
    any application-level query/filtering logic entirely, which makes it useful
    for debugging the raw, unfiltered graph.

    Returns the RDF/JSON response body (``{subjectIri: {predicateIri: [{value,
    type, datatype?, lang?}, ...]}}``), or an empty dict if the IRI has no
    triples.
    """
    response = await request(
        db.server_id,
        "GET",
        "/v1/graphs/things",
        params={"iri": iri, "database": db.database},
        headers={"Accept": "application/rdf+json"},
    )

    if response.status_code == 404:
        return {}
    if response.status_code >= 400:
        raise MarkLogicError(f"Describe IRI failed: {response.status_code} {response.text}")
    if not response.content:
        return {}

    try:
        result = response.json()
    except ValueError as exc:
        content_type = response.headers.get("content-type", "")
        raise MarkLogicError(
            f"Describe IRI response was not JSON (content-type={content_type!r})"
        ) from exc
    if not isinstance(result, dict):
        raise MarkLogicError("Describe IRI response was not a JSON object")
    return result


@retry_on_error
async def admin_rows(
    db: MarkLogicDatabaseLocator,
    *,
    dsl: str | None = None,
    plan: dict | None = None,
    sql: str | None = None,
    sparql: str | None = None,
    graphql: str | None = None,
    params: dict | None = None,
) -> list[dict]:
    """Always uses the admin client regardless of account context. Use for internal/admin queries."""
    client = get_admin_marklogic_client(db.server_id)

    params = {**(params or {}), "database": db.database}
    query_kwargs: dict = {
        "dsl": dsl,
        "plan": plan,
        "sql": sql,
        "sparql": sparql,
        "graphql": graphql,
        "params": params,
    }

    result = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(client.rows.query, **query_kwargs),
    )
    if isinstance(result, requests.Response):
        check_marklogic_response(result, "execute rows query")

    return result


async def search_iter(
    db: MarkLogicDatabaseLocator,
    *,
    collection: str | None = None,
    batch_size: int = 100,
    q: str | None = None,
    query: dict | str | None = None,
    categories: list[str] | None = None,
    directory: str | None = None,
) -> AsyncGenerator[Document, None]:
    start = 1
    while True:
        results = await search(
            db,
            collection=collection,
            start=start,
            limit=batch_size,
            q=q,
            query=query,
            categories=categories,
            directory=directory,
        )
        if len(results) == 0:
            break
        for doc in results:
            yield doc
        start += batch_size


async def search_batch(
    db: MarkLogicDatabaseLocator,
    *,
    collection: str | None = None,
    batch_size: int = 100,
    q: str | None = None,
    query: dict | str | None = None,
    categories: list[str] | None = None,
    directory: str | None = None,
) -> AsyncGenerator[list[Document], None]:
    batch = []
    async for doc in search_iter(
        db,
        collection=collection,
        batch_size=batch_size,
        q=q,
        query=query,
        categories=categories,
        directory=directory,
    ):
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch


@retry_on_error
async def retrieve(
    db: MarkLogicDatabaseLocator,
    payload: MraRetrieveRequest,
) -> MraRetrieveResponse:
    """Call MRA /v1/retrieve on the target MarkLogic data server."""
    client = get_httpx_system_client_for_context(db.server_id)
    resp = await client.post("/v1/retrieve", json=payload, params={"database": db.database})
    if not resp.is_success:
        check_marklogic_response(resp, "retrieve documents")
    return resp.json() if resp.content else {}


@retry_on_error
async def retrieve_definition(
    db: MarkLogicDatabaseLocator,
) -> MraRetrieveDefinitionResponse:
    """Call MRA /v1/retrieve/definition on the target MarkLogic data server."""
    client = get_httpx_system_client_for_context(db.server_id)
    resp = await client.get("/v1/retrieve/definition", params={"database": db.database})
    if not resp.is_success:
        check_marklogic_response(resp, "retrieve definition")
    return resp.json() if resp.content else {}


async def stream_document(
    db: MarkLogicDatabaseLocator,
    uri: str,
    *,
    chunk_size: int = 1024 * 512,
) -> AsyncGenerator[bytes, None]:
    """Stream the raw bytes of a single MarkLogic document without buffering the full body.

    Uses an async httpx client with the same credentials as the requests-based client
    for the current user/account context.  Yields ``chunk_size``-byte chunks as they
    arrive from the server.

    Raises ``MarkLogicError`` if the server returns a non-200 status.
    Raises ``NoHealthyUpstreamError`` (a subclass of ``MarkLogicError``) on 503 no healthy upstream.
    """
    client = get_httpx_system_client_for_context(db.server_id)
    params = {"uri": uri, "database": db.database}
    async with client.stream("GET", "/v1/documents", params=params) as resp:
        if resp.status_code != 200:
            await resp.aread()
            check_marklogic_response(resp, "stream document {uri!r}")
        async for chunk in resp.aiter_bytes(chunk_size):
            yield chunk


async def _multipart_body(
    boundary: bytes,
    uri: str,
    content_type: str,
    permissions: dict[str, list[str]] | None,
    collections: list[str] | None,
    body_iter: AsyncGenerator[bytes, None],
) -> AsyncGenerator[bytes, None]:
    """Yield the raw bytes of a multipart/mixed POST body for a single document.

    Optionally prefixes a metadata part when permissions or collections are provided.
    The document content part is streamed directly from ``body_iter`` so the caller's
    data never needs to be fully buffered.
    """
    dashes = b"--"
    crlf = b"\r\n"

    if permissions is not None or collections is not None:
        metadata: dict = {}
        if permissions is not None:
            metadata["permissions"] = [
                {"role-name": k, "capabilities": v} for k, v in permissions.items()
            ]
        if collections is not None:
            metadata["collections"] = collections
        meta_bytes = json.dumps(metadata).encode()
        yield dashes + boundary + crlf
        yield (
            f'Content-Disposition: attachment; filename="{uri}"; category=metadata\r\n'
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(meta_bytes)}\r\n"
            f"\r\n"
        ).encode()
        yield meta_bytes
        yield crlf

    yield dashes + boundary + crlf
    yield (
        f'Content-Disposition: attachment; filename="{uri}"\r\nContent-Type: {content_type}\r\n\r\n'
    ).encode()
    async for chunk in body_iter:
        yield chunk
    yield crlf

    yield dashes + boundary + b"--" + crlf


@retry_on_error
async def _post_stream_write(
    db: MarkLogicDatabaseLocator,
    uri: str,
    stream: _ReplayableAsyncFileStream,
    multipart_content_type: str,
    content_length: int,
) -> None:
    client = get_httpx_system_client_for_context(db.server_id)
    resp = await client.post(
        "/v1/documents",
        content=stream,
        headers={
            "Content-Type": multipart_content_type,
            "Content-Length": str(content_length),
            "Accept": "application/json",
        },
        params={"database": db.database},
    )
    if not resp.is_success:
        check_marklogic_response(resp, f"stream-write document {uri!r}")


async def stream_write(
    db: MarkLogicDatabaseLocator,
    uri: str,
    body_iter: AsyncGenerator[bytes, None],
    *,
    content_type: str = "application/octet-stream",
    permissions: dict[str, list[str]] | None = None,
    collections: list[str] | None = None,
) -> None:
    """Write a document to MarkLogic using a replayable spooled request body.

    ``body_iter`` is an async generator of raw bytes that will be sent directly
    to MarkLogic as the document body inside a multipart/mixed POST request.

    The multipart body spills to disk above 8 MiB so DigestAuth and transient-error
    retries can replay the request without retaining large uploads in memory.

    Args:
        db: Target database locator.
        uri: Document URI to write.
        body_iter: Async generator yielding the document content as bytes chunks.
        content_type: MIME type of the document. Defaults to ``application/octet-stream``.
        permissions: Optional dict of ``{role-name: [capabilities]}`` to set on the document.
        collections: Optional list of collection URIs to assign the document to.

    Raises:
        MarkLogicError: if MarkLogic returns a non-2xx response.
    """
    boundary = uuid.uuid4().hex.encode()
    multipart_content_type = f"multipart/mixed; boundary={boundary.decode()}"

    # DigestAuth can send the request twice. Keep a replayable multipart body,
    # spilling larger uploads to disk instead of retaining the whole file in memory.
    with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as body:
        async for chunk in _multipart_body(
            boundary, uri, content_type, permissions, collections, body_iter
        ):
            body.write(chunk)
        content_length = body.tell()
        stream = _ReplayableAsyncFileStream(body)
        await _post_stream_write(
            db,
            uri,
            stream,
            multipart_content_type,
            content_length,
        )


@retry_on_error
async def eval(
    db: MarkLogicDatabaseLocator,
    *,
    javascript: str | None = None,
    xquery: str | None = None,
    vars: dict | None = None,
) -> list:
    client = get_client_for_context(db.server_id)
    eval_kwargs: dict[str, Any] = {"params": {"database": db.database}}
    if javascript is not None:
        eval_kwargs["javascript"] = javascript
    if xquery is not None:
        eval_kwargs["xquery"] = xquery
    if vars is not None:
        eval_kwargs["vars"] = vars
    result = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(client.eval, **eval_kwargs),
    )
    if isinstance(result, requests.Response):
        check_marklogic_response(result, "eval")
    return result or []


async def get_document_permissions(db: MarkLogicDatabaseLocator, uri: str) -> dict[str, list[str]]:
    """Return a dict of {role-name: [capabilities]} for the given document URI."""
    js = """
    const perms = xdmp.documentGetPermissions(uri);
    const result = {};
    for (const perm of perms) {
      const role = xdmp.roleName(perm.roleId);
      if (!result[role]) result[role] = [];
      result[role].push(perm.capability);
    }
    result;
    """
    results = await eval(db, javascript=js, vars={"uri": uri})
    perms: dict[str, list[str]] = results[0] if results else {}
    return {role: sorted(caps) for role, caps in perms.items()}


@retry_on_error
async def get_role_users(
    *, server_id: str, role_prefix: str, account_id: str
) -> dict[str, dict]:  # pragma: no cover
    """Return a mapping of user_id -> {"level", "description", "type"}.

    Calls the role-users REST extension, which queries both <prefix>-reader and
    <prefix>-writer roles against the Security database via an amp.
    """
    client = get_client_for_context(server_id)
    loop = asyncio.get_running_loop()
    resp = await loop.run_in_executor(
        get_marklogic_executor(),
        partial(
            client.get,
            "/v1/resources/role-users",
            params={"rs:role-prefix": role_prefix, "rs:account-id": account_id},
        ),
    )
    if resp.status_code != 200:
        check_marklogic_response(resp, "get users for role prefix {role_prefix!r}")
    return resp.json()


# TTL cache for per-user role lookups: {user_key: (roles, expiry_timestamp)}
# Bounded to 1000 entries via LRU eviction to prevent unbounded memory growth.
_pdp_roles_cache: LRU = LRU(1000)
_PDP_ROLES_CACHE_TTL = 300  # 5 minutes


@retry_on_error
async def get_user_roles(*, server_id: str, user_id: str, account_id: str) -> list[str]:
    """
    Return all PDP role names the user holds (including inherited), cached for 5 minutes.
    These roles are used by the UI to determine what to show to a user. It is fine if a cache
    entry is stale for a few minutes, because each route will always verify that the user
    still has a required privilege via a call to MarkLogic.

    Note that the REST extension strips off the "pdp-" prefix. That prefix is used solely to make
    it easier to manage the roles in MarkLogic.
    """
    cache_key = f"{user_id}:{account_id}"
    cached = _pdp_roles_cache.get(cache_key)
    if cached is not None:
        roles, expiry = cached
        if time.monotonic() < expiry:
            return roles

    client = get_client_for_context(server_id)
    loop = asyncio.get_running_loop()
    resp = await loop.run_in_executor(
        get_marklogic_executor(),
        partial(client.get, "/v1/resources/user-pdp-roles"),
    )
    if resp.status_code != 200:
        check_marklogic_response(resp, f"get pdp roles for user {user_id!r}")
    roles = resp.json()
    now = time.monotonic()
    _pdp_roles_cache[cache_key] = (roles, now + _PDP_ROLES_CACHE_TTL)
    # Prune expired entries so they don't linger until evicted by LRU pressure.
    stale_keys = [k for k, (_, expiry) in _pdp_roles_cache.items() if expiry < now]
    for k in stale_keys:
        del _pdp_roles_cache[k]
    return roles


def invalidate_pdp_roles_cache(*, user_id: str, account_id: str) -> None:
    """Evict a user's pdp-roles cache entry so the next request fetches fresh data."""
    cache_key = f"{user_id}:{account_id}"
    _pdp_roles_cache.pop(cache_key, None)


@retry_on_error
async def security_assert(*, server_id: str, privilege: str) -> bool:
    """Return True if the current MarkLogic user holds the named execute privilege, False if not."""
    client = get_client_for_context(server_id)
    loop = asyncio.get_running_loop()
    resp = await loop.run_in_executor(
        get_marklogic_executor(),
        partial(client.get, "/v1/resources/security-assert", params={"rs:privilege": privilege}),
    )
    if resp.status_code == 200:
        return True
    if resp.status_code == 403:
        return False
    check_marklogic_response(resp, f"security-assert privilege {privilege!r}")
    return False  # unreachable; check_marklogic_response always raises
