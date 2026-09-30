import asyncio
import enum
import json
import logging
from dataclasses import dataclass, field
from functools import partial

from marklogic.documents import DefaultMetadata, Document  # type: ignore[import-untyped]

from .exceptions import MarkLogicError
from .models import MarkLogicDatabaseLocator
from .utils import (
    check_marklogic_response,
    get_admin_marklogic_client,
    get_marklogic_executor,
    retry_on_error,
    system_request,
)

logger = logging.getLogger(__name__)

# Every TDE (row or triple) template document is written to both of these collections.
_TDE_COLLECTIONS = ["http://marklogic.com/xdmp/tde", "TDE"]
_verified_templates: dict[tuple[str, str, str], str] = {}


class ScalarType(str, enum.Enum):
    STRING = "string"
    INT = "int"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    DATETIME = "dateTime"
    DECIMAL = "decimal"
    VECTOR = "vector"


@dataclass
class Column:
    name: str
    scalar_type: ScalarType
    val_path: str
    dimension: int | None = None
    invalid_values: str | None = None
    nullable: bool = False
    virtual: bool | None = None


@dataclass
class TDE:
    schema_name: str
    view_name: str
    collections: list[str]
    rows: list[Column]
    context: str = "/"
    view_layout: str | None = None
    view_virtual: bool | None = None
    namespaces: dict[str, str] | None = None
    template_name: str | None = None
    """Distinguishes this template's document URI from another TDE targeting the same
    ``schema_name``/``view_name`` (e.g. one JSON-only and one XML-only template
    contributing rows to the same SQL view). Defaults to ``f"{schema_name}-{view_name}"``
    when unset, matching every existing single-template-per-view usage. Two templates
    sharing a ``schema_name``/``view_name`` but *not* given distinct ``template_name``s
    would collide on the same document URI and silently overwrite each other; worse,
    unioning them into one wildcard-terminated context (rather than splitting into two
    templates) causes MarkLogic to raise ``TDE-EVALFAILED: ... returns multiple rows for
    the same view/doc/context node over multiple templates`` at eval time, since it treats
    a wildcard union as ambiguous internally even though it registers without error."""


# ---------------------------------------------------------------------------
# Triple (semantic) templates
# ---------------------------------------------------------------------------


@dataclass
class TripleNode:
    """One position (subject/predicate/object) of a projected triple.

    ``val`` is a TDE-dialect XQuery/XPath expression evaluated relative to the
    template ``context``. ``invalid_values`` controls what happens when the
    expression cannot be evaluated (e.g. ``"ignore"`` to drop the triple).
    """

    val: str
    invalid_values: str | None = None


@dataclass
class Triple:
    subject: TripleNode
    predicate: TripleNode
    object: TripleNode


@dataclass
class TripleVar:
    """An intermediate value extracted at the current context level.

    Referenced from node expressions as ``$name``.
    """

    name: str
    val: str


@dataclass
class TripleTemplate:
    """A triple-extraction TDE template.

    Projects one or more triples per ``context`` match, scoped to ``collections``.
    Templates that share the same triple store are queryable together as a single
    logical graph.
    """

    name: str
    context: str
    collections: list[str]
    triples: list[Triple]
    vars: list[TripleVar] = field(default_factory=list)
    namespaces: dict[str, str] | None = None


def _build_column_spec(col: Column) -> dict:
    spec: dict = {
        "name": col.name,
        "scalarType": col.scalar_type.value,
        "val": col.val_path,
    }
    if col.dimension is not None:
        spec["dimension"] = col.dimension
    if col.invalid_values is not None:
        spec["invalidValues"] = col.invalid_values
    if col.nullable:
        spec["nullable"] = True
    if col.virtual is not None:
        spec["virtual"] = col.virtual
    return spec


def _build_node_spec(node: TripleNode) -> dict:
    spec: dict = {"val": node.val}
    if node.invalid_values is not None:
        spec["invalidValues"] = node.invalid_values
    return spec


def _build_triple_spec(triple: Triple) -> dict:
    return {
        "subject": _build_node_spec(triple.subject),
        "predicate": _build_node_spec(triple.predicate),
        "object": _build_node_spec(triple.object),
    }


async def _get_template(db: MarkLogicDatabaseLocator, uri: str) -> dict | None:
    """Return the currently installed template JSON at ``uri``, or ``None``.

    Used to skip redundant writes: a TDE PUT triggers reindexing, so we avoid
    rewriting a template whose content is already identical.
    """
    resp = await system_request(
        db.server_id,
        "GET",
        "/v1/documents",
        params={"database": db.database, "uri": uri},
        headers={"Accept": "application/json"},
    )
    if resp.status_code == 404 or len(resp.content) == 0:
        return None
    if resp.status_code != 200:
        # Treat any other read failure as "unknown" and let the caller PUT.
        return None
    try:
        parsed = resp.json()
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


@retry_on_error
async def _get_templates(db: MarkLogicDatabaseLocator, uris: list[str]) -> dict[str, dict]:
    """Bulk-read the currently installed templates at ``uris`` in a single request.

    Returns ``{uri: template}`` for whichever of ``uris`` currently exist; missing URIs are
    simply absent from the result (not an error) — used by :func:`load_templates` to skip
    redundant writes (a TDE PUT triggers reindexing) across many templates in one round trip
    instead of one GET per template.
    """
    if not uris:
        return {}
    client = get_admin_marklogic_client(db.server_id)
    res = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(
            client.documents.search,
            query={"ctsquery": {"documentQuery": {"uris": uris}}},
            categories=["content"],
            params={"database": db.database},
            page_length=len(uris),
        ),
    )
    if not isinstance(res, list):
        if res.status_code == 400 and "No such database" in res.text:
            # Treat "No such database" as "not found" for all URIs.
            return {}
        check_marklogic_response(res, "read TDE templates")
    return {doc.uri: doc.content for doc in res if isinstance(doc.content, dict)}


async def _put_template(db: MarkLogicDatabaseLocator, uri: str, template: dict) -> None:
    # Skip the write (and the reindex it triggers) when the installed template
    # already matches the desired content.
    if await _get_template(db, uri) == template:
        logger.debug("TDE template unchanged, skipping install", extra={"uri": uri})
        return

    resp = await system_request(
        db.server_id,
        "PUT",
        "/v1/documents",
        params={
            "database": db.database,
            "uri": uri,
            "collection": _TDE_COLLECTIONS,
        },
        headers={"Content-Type": "application/json"},
        data=json.dumps(template),
    )
    if resp.status_code not in (200, 201, 204):  # pragma: no cover
        logger.error(
            "Failed to install TDE", extra={"status_code": resp.status_code, "response_text": resp.text}
        )
        raise MarkLogicError(f"Failed to install TDE: {resp.status_code} {resp.text}")


@retry_on_error
async def _write_templates(db: MarkLogicDatabaseLocator, templates: dict[str, dict]) -> None:
    """Bulk-write ``{uri: template}`` in a single POST, all sharing the same collections."""
    client = get_admin_marklogic_client(db.server_id)
    default_metadata = DefaultMetadata(collections=_TDE_COLLECTIONS)
    documents = [
        Document(uri, template, content_type="application/json") for uri, template in templates.items()
    ]
    resp = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(
            client.documents.write,
            [default_metadata, *documents],
            params={"database": db.database},
        ),
    )
    if not resp.ok:
        check_marklogic_response(resp, "bulk-install TDE templates")


def _tde_template_uri(tde: TDE) -> str:
    return f"/tde/{tde.template_name or f'{tde.schema_name}-{tde.view_name}'}.json"


def _build_tde_template(tde: TDE) -> dict:
    row: dict = {
        "schemaName": tde.schema_name,
        "viewName": tde.view_name,
        "columns": [_build_column_spec(col) for col in tde.rows],
    }
    if tde.view_layout is not None:
        row["viewLayout"] = tde.view_layout
    if tde.view_virtual is not None:
        row["viewVirtual"] = tde.view_virtual
    template: dict = {
        "context": tde.context,
        "collections": tde.collections,
        "rows": [row],
    }
    if tde.namespaces is not None:
        template["pathNamespace"] = [
            {"prefix": prefix, "namespaceUri": uri} for prefix, uri in tde.namespaces.items()
        ]
    return {"template": template}


def _triple_template_uri(name: str) -> str:
    return f"/tde/graph/{name}.json"


def _build_triple_template(template: TripleTemplate) -> dict:
    body: dict = {
        "context": template.context,
        "collections": template.collections,
        "triples": [_build_triple_spec(triple) for triple in template.triples],
    }
    if template.vars:
        body["vars"] = [{"name": var.name, "val": var.val} for var in template.vars]
    if template.namespaces is not None:
        body["pathNamespace"] = [
            {"prefix": prefix, "namespaceUri": uri} for prefix, uri in template.namespaces.items()
        ]
    return {"template": body}


async def install_triples(db: MarkLogicDatabaseLocator, template: TripleTemplate) -> None:
    """Install a triple-extraction TDE template into the schema database."""
    await _put_template(db, _triple_template_uri(template.name), _build_triple_template(template))


async def load_templates(
    db: MarkLogicDatabaseLocator,
    tdes: list[TDE] | None = None,
    triple_templates: list[TripleTemplate] | None = None,
) -> None:
    """Install any number of row (TDE) and/or triple-extraction templates in one request.

    Batches the "is this template already installed with this exact content?" read and the
    write of every changed template into a single round trip each, rather than one GET + one
    PUT per template — meaningful when installing many templates at once (e.g. project schema
    initialization installs ~15 templates), since each round trip against MarkLogic previously
    cost roughly 1-1.5s serially.
    """
    templates_by_uri = {_tde_template_uri(tde): _build_tde_template(tde) for tde in tdes or []}
    templates_by_uri.update(
        {
            _triple_template_uri(template.name): _build_triple_template(template)
            for template in triple_templates or []
        }
    )
    if not templates_by_uri:
        return

    serialized = {
        uri: json.dumps(template, sort_keys=True, separators=(",", ":"))
        for uri, template in templates_by_uri.items()
    }
    pending = {
        uri: template
        for uri, template in templates_by_uri.items()
        if _verified_templates.get((db.server_id, db.database, uri)) != serialized[uri]
    }
    if not pending:
        logger.debug(
            "All TDE templates already verified locally, skipping lookup",
            extra={"uris": list(templates_by_uri)},
        )
        return

    existing = await _get_templates(db, list(pending.keys()))
    changed = {uri: template for uri, template in pending.items() if existing.get(uri) != template}
    if not changed:
        logger.debug("All TDE templates unchanged, skipping bulk install", extra={"uris": list(pending)})
    else:
        await _write_templates(db, changed)

    for uri in pending:
        _verified_templates[(db.server_id, db.database, uri)] = serialized[uri]


async def uninstall_triples(db: MarkLogicDatabaseLocator, name: str) -> None:
    """Remove a previously installed triple-extraction TDE template."""
    resp = await system_request(
        db.server_id,
        "DELETE",
        "/v1/documents",
        params={"database": db.database, "uri": _triple_template_uri(name)},
    )
    if resp.status_code not in (200, 204, 404):  # pragma: no cover
        logger.error(
            "Failed to uninstall TDE",
            extra={"status_code": resp.status_code, "response_text": resp.text},
        )
        raise MarkLogicError(f"Failed to uninstall TDE: {resp.status_code} {resp.text}")


async def delete_templates(db: MarkLogicDatabaseLocator, names: list[str]) -> None:
    """Remove any number of previously installed triple-extraction TDE templates in one request."""
    if not names:
        return
    resp = await system_request(
        db.server_id,
        "DELETE",
        "/v1/documents",
        params={"database": db.database, "uri": [_triple_template_uri(name) for name in names]},
    )
    if resp.status_code not in (200, 204, 404):  # pragma: no cover
        logger.error(
            "Failed to bulk-uninstall TDE templates",
            extra={"status_code": resp.status_code, "response_text": resp.text},
        )
        raise MarkLogicError(f"Failed to bulk-uninstall TDE templates: {resp.status_code} {resp.text}")
