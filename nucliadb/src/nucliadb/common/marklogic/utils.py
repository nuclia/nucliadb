import asyncio
import concurrent.futures
import contextlib
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextvars import ContextVar
from functools import lru_cache, partial, wraps
from typing import ParamSpec, TypeVar

import backoff
import httpx
import requests
from lru import LRU
from marklogic import Client  # type: ignore[import-untyped]
from requests.adapters import HTTPAdapter
from yarl import URL

from .exceptions import DatabaseDoesNotExist, MarkLogicError, NoHealthyUpstreamError, RoleDoesNotExist
from .settings import META_MARKLOGIC_SERVER_ID, env_settings

logger = logging.getLogger(__name__)

_NO_HEALTHY_UPSTREAM_PHRASES = ("no healthy upstream", "remote connection failure")


def check_marklogic_response(resp: httpx.Response | requests.Response, action_type: str) -> None:
    """Inspect *resp* and raise the most specific ``MarkLogicError`` subclass available.

    Currently recognised transient errors:
    - 503 containing "no healthy upstream" or "remote connection failure" → ``NoHealthyUpstreamError``
    - ``XDMP-NOSUCHDB`` → ``DatabaseDoesNotExist``
    - ``SEC-ROLEDNE`` → ``RoleDoesNotExist``

    For all other non-successful responses the generic ``MarkLogicError`` is raised
    with *action_type* included in the message so callers don't need to construct
    their own error strings.

    Args:
        resp: The HTTP response to check.
        action_type: Short description of the operation (e.g. ``"write document"``),
            used in the fallback ``MarkLogicError`` message.
    """
    if resp.status_code == 503 and any(
        phrase in resp.text.lower() for phrase in _NO_HEALTHY_UPSTREAM_PHRASES
    ):
        raise NoHealthyUpstreamError(f"MarkLogic returned 503 no healthy upstream: {resp.text}")
    if "XDMP-NOSUCHDB" in resp.text:
        raise DatabaseDoesNotExist(f"MarkLogic database does not exist: {resp.text}")
    if "SEC-ROLEDNE" in resp.text:
        raise RoleDoesNotExist(f"MarkLogic role does not exist: {resp.text}")
    raise MarkLogicError(f"Failed to {action_type}: {resp.status_code} {resp.text}")


def retry_on_error(func):
    """Decorator: retry the wrapped async function on retriable ``MarkLogicError`` subclasses.

    Currently retries on:
    - ``NoHealthyUpstreamError`` (503 no healthy upstream — transient load-balancer error).
      Uses exponential back-off with full jitter, up to 6 attempts, capped at 10 seconds total.
    - ``RoleDoesNotExist`` (SEC-ROLEDNE) — MarkLogic's security database is only eventually
      consistent across cluster hosts: a role created via the Management API is committed on
      the host that served that request, then replicated to other hosts on the cluster's
      heartbeat/security-cache-refresh cycle (1 second by default). If the very next request
      (e.g. a document write referencing that role's permissions) lands on a different host
      via the load balancer that hasn't yet caught up, it transiently sees the role as
      missing. Retrying briefly (well under a couple of heartbeats) gives that host time to
      catch up, without every caller needing its own wait/retry logic. If the role is
      genuinely gone for good (e.g. deleted concurrently), retries simply exhaust quickly and
      the error propagates as before. Uses a short constant back-off (250ms, up to 8 attempts,
      ~1.75s total) rather than the longer exponential schedule used for the upstream-health
      case, since this only needs to outlast a heartbeat or two, not a load-balancer outage.
    """
    role_retry = backoff.on_exception(
        backoff.constant,
        RoleDoesNotExist,
        max_tries=8,
        interval=0.25,
        jitter=None,
        on_backoff=lambda details: logger.warning(
            "MarkLogic role not yet visible (replication lag), retrying",
            extra={
                "wait": details["wait"],
                "tries": details["tries"],
                "exception_type": type(details["exception"]).__name__,
            },
            exc_info=details["exception"],
        ),
    )
    upstream_retry = backoff.on_exception(
        backoff.expo,
        NoHealthyUpstreamError,
        max_tries=6,
        max_time=10,
        jitter=backoff.full_jitter,
        on_backoff=lambda details: logger.warning(
            "MarkLogic retriable error, retrying",
            extra={
                "wait": details["wait"],
                "tries": details["tries"],
                "exception_type": type(details["exception"]).__name__,
            },
            exc_info=details["exception"],
        ),
    )
    return role_retry(upstream_retry(func))


MARKLOGIC_MAX_WORKERS = 50
# Each requests.Session has a default pool_maxsize of 10. With up to
# MARKLOGIC_MAX_WORKERS threads sharing a single Client instance (admin client),
# connections overflow and get discarded. Size the pool to match the thread pool.
_POOL_MAXSIZE = MARKLOGIC_MAX_WORKERS


def _configure_session_pool(client: Client, pool_maxsize: int = _POOL_MAXSIZE) -> None:
    """Mount an HTTPAdapter with a larger connection pool on a marklogic.Client session."""
    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=pool_maxsize)
    client.mount("http://", adapter)
    client.mount("https://", adapter)


# Stores the current request's user/account IDs so worker threads can look up the right client.
_user_id_ctx: ContextVar[str | None] = ContextVar("marklogic_user_id", default=None)
_account_id_ctx: ContextVar[str | None] = ContextVar("marklogic_account_id", default=None)
_admin_ctx: ContextVar[bool] = ContextVar("marklogic_admin", default=False)


def set_marklogic_user_id(user_id: str | None) -> None:
    _user_id_ctx.set(user_id)


def set_marklogic_account_id(account_id: str | None) -> None:
    _account_id_ctx.set(account_id)


@contextlib.asynccontextmanager
async def with_user(*, user_id: str, account_id: str) -> AsyncGenerator[None, None]:
    """Async context manager that sets the MarkLogic user/account context for the duration of the block.

    Use this in migrations and background tasks to perform DB operations as a
    specific user, so that document-level permissions are enforced correctly.

    Example::

        async with with_user(user_id=user_id, account_id=account_id):
            await some_repository_function(...)
    """
    token_user = _user_id_ctx.set(user_id)
    token_account = _account_id_ctx.set(account_id)
    try:
        yield
    finally:
        _user_id_ctx.reset(token_user)
        _account_id_ctx.reset(token_account)


@contextlib.asynccontextmanager
async def with_admin_client() -> AsyncGenerator[None, None]:
    """Async context manager that explicitly opts in to admin-level MarkLogic access.

    Use this in background consumers/tasks that process events not tied to any single end
    user (e.g. a NATS consumer handling an event that could belong to any account), where
    there's no specific user to run as via `with_user(...)`. This makes the elevated-access
    choice explicit and visible at the call site, rather than something that happens
    silently whenever user/account context isn't set.

    Clears any outer user/account context for the duration of the block, so this reliably
    grants admin access even when nested inside `with_user(...)` (e.g. test setup code that
    needs to bypass per-user permissions while a test otherwise runs as a specific user).
    Nesting `with_user(...)` inside `with_admin_client()` still switches back to that user for
    the inner block, as usual.
    """
    token_user = _user_id_ctx.set(None)
    token_account = _account_id_ctx.set(None)
    token_admin = _admin_ctx.set(True)
    try:
        yield
    finally:
        _admin_ctx.reset(token_admin)
        _account_id_ctx.reset(token_account)
        _user_id_ctx.reset(token_user)


_HandlerParams = ParamSpec("_HandlerParams")
_HandlerReturnT = TypeVar("_HandlerReturnT")


def with_admin_client_handler(
    func: Callable[_HandlerParams, Awaitable[_HandlerReturnT]],
) -> Callable[_HandlerParams, Awaitable[_HandlerReturnT]]:
    """Decorator that runs the wrapped async function entirely under `with_admin_client()`.

    Intended for background message-consumer handlers (e.g. `messagebus.Consumer.handler`)
    that process events not tied to any single end user, so there's no per-request
    user/account context available. Wrapping the whole handler (rather than individual calls
    inside it) guarantees every marklogic call it makes -- including calls several layers deep
    in code it invokes -- runs with admin access, without having to track down and wrap each
    one individually.

    Example::

        @marklogic.with_admin_client_handler
        async def handle_dataset_deleted(envelope: MessageEnvelope) -> None:
            ...
    """

    @wraps(func)
    async def wrapper(*args: _HandlerParams.args, **kwargs: _HandlerParams.kwargs) -> _HandlerReturnT:
        async with with_admin_client():
            return await func(*args, **kwargs)

    return wrapper


_user_client_lru_cache: LRU = LRU(50)


def get_user_marklogic_client(server_id: str, user_id: str, account_id: str) -> Client:
    """Digest-auth client on port 8003 authenticated as pdp-user-{user_id}-{account_id}.

    MarkLogic document permissions are enforced for this user, scoping access to the account.
    """
    cache_key = (server_id, user_id, account_id)
    cached = _user_client_lru_cache.get(cache_key)
    if cached is not None:
        return cached

    server_settings = env_settings.get_marklogic_server(server_id)
    parsed_uri = URL(server_settings.uri)
    system_uri = str(parsed_uri.with_port(server_settings.system_port))
    username = f"pdp-user-{user_id}-{account_id}"
    client = Client(system_uri, digest=(username, username))
    _configure_session_pool(client)
    _user_client_lru_cache[cache_key] = client
    return client


@lru_cache
def get_admin_marklogic_client(server_id: str) -> Client:
    """Digest-auth admin client on port 8003. Bypasses document permissions — use for system operations only."""
    server_settings = env_settings.get_marklogic_server(server_id)
    assert server_settings.username is not None, f"username is required for MarkLogic server {server_id}"
    assert server_settings.password is not None, f"password is required for MarkLogic server {server_id}"
    parsed_uri = URL(server_settings.uri)
    system_uri = str(parsed_uri.with_port(server_settings.system_port))
    client = Client(
        system_uri,
        digest=(
            server_settings.username,
            server_settings.password.get_secret_value(),
        ),
    )
    _configure_session_pool(client)
    return client


def get_client_for_context(server_id: str) -> Client:
    """Return the per-user client if user/account context is set, else the admin client.

    Raises if neither the user/account context nor the explicit admin context is set, since
    non-admin marklogic client calls must run within a request (user/account context set by
    middleware), inside `with_user(...)`, or inside `with_admin_client()` for background/system
    code that intentionally needs elevated access. There is no silent fallback: previously this
    quietly returned the admin client (bypassing per-user document permissions) whenever context
    was unset, which could silently broaden access for any caller that forgot to set up context.
    """
    user_id = _user_id_ctx.get()
    account_id = _account_id_ctx.get()
    if user_id and account_id:
        return get_user_marklogic_client(server_id, user_id, account_id)
    if _admin_ctx.get():
        return get_admin_marklogic_client(server_id)
    raise RuntimeError(
        "MarkLogic user/account context not set outside of with_user()/with_admin_client()/"
        "request context. Non-admin marklogic client calls must run within a request (user/"
        "account context set by middleware), inside with_user(...), or inside "
        "with_admin_client() for background/system code that intentionally needs elevated "
        "access."
    )


@lru_cache
def get_httpx_admin_client(server_id: str) -> httpx.AsyncClient:
    server_settings = env_settings.get_marklogic_server(server_id)
    assert server_settings.username is not None, f"username is required for MarkLogic server {server_id}"
    assert server_settings.password is not None, f"password is required for MarkLogic server {server_id}"
    admin_base_url = URL(server_settings.uri).with_port(server_settings.admin_port)
    client = httpx.AsyncClient(
        auth=httpx.DigestAuth(
            server_settings.username,
            server_settings.password.get_secret_value(),
        ),
        base_url=admin_base_url.human_repr(),
        timeout=60.0 * 5.0,
    )
    return client


_system_lru_cache: LRU = LRU(50)


def get_httpx_system_client(
    server_id: str, user_id: str | None, account_id: str | None
) -> httpx.AsyncClient:
    """Return a per-user (or admin) async httpx client targeting the system port.

    Credentials and base URL are derived from the same source as the requests-based
    clients so that auth is consistent.  Results are cached by (server_id, user_id,
    account_id) so each unique combination gets exactly one long-lived client.

    Pass user_id=None / account_id=None to get the admin client.
    """
    cache_key = (server_id, user_id, account_id)
    cached = _system_lru_cache.get(cache_key)
    if cached is not None:
        return cached

    server_settings = env_settings.get_marklogic_server(server_id)
    assert server_settings.username is not None, f"username is required for MarkLogic server {server_id}"
    assert server_settings.password is not None, f"password is required for MarkLogic server {server_id}"
    system_base_url = URL(server_settings.uri).with_port(server_settings.system_port)

    if user_id and account_id:
        username = f"pdp-user-{user_id}-{account_id}"
        auth = httpx.DigestAuth(username, username)
    else:
        password = server_settings.password.get_secret_value()
        auth = httpx.DigestAuth(server_settings.username, password)

    client = httpx.AsyncClient(auth=auth, base_url=system_base_url.human_repr(), timeout=60.0 * 5.0)
    _system_lru_cache[cache_key] = client
    return client


def get_httpx_system_client_for_context(server_id: str) -> httpx.AsyncClient:
    """Return the per-user httpx system client when context vars are set, else admin."""
    user_id = _user_id_ctx.get()
    account_id = _account_id_ctx.get()
    return get_httpx_system_client(server_id, user_id, account_id)


class _ContextCopyingThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    """ThreadPoolExecutor that propagates MarkLogic ContextVars to worker threads."""

    def submit(self, fn, /, *args, **kwargs):
        user_id = _user_id_ctx.get()
        account_id = _account_id_ctx.get()

        def wrapper():
            _user_id_ctx.set(user_id)
            _account_id_ctx.set(account_id)
            return fn(*args, **kwargs)

        return super().submit(wrapper)


@lru_cache
def get_marklogic_executor() -> concurrent.futures.ThreadPoolExecutor:
    return _ContextCopyingThreadPoolExecutor(max_workers=MARKLOGIC_MAX_WORKERS)


async def close():
    close_tasks = [get_httpx_admin_client(META_MARKLOGIC_SERVER_ID).aclose()]
    for server_settings in env_settings.marklogic_data_servers:
        close_tasks.append(get_httpx_admin_client(server_settings.id).aclose())

    for client in _system_lru_cache.values():
        close_tasks.append(client.aclose())

    await asyncio.gather(*close_tasks)

    get_httpx_admin_client.cache_clear()
    _system_lru_cache.clear()
    _user_client_lru_cache.clear()
    get_admin_marklogic_client.cache_clear()


async def request(server_id: str, method: str, path: str, **kwargs) -> httpx.Response:
    client = get_client_for_context(server_id)
    resp = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(client.request, method, path, **kwargs),
    )
    return resp


async def system_request(server_id: str, method: str, path: str, **kwargs) -> httpx.Response:
    """Always uses the admin client regardless of user context. Use for admin operations."""
    client = get_admin_marklogic_client(server_id)
    resp = await asyncio.get_running_loop().run_in_executor(
        get_marklogic_executor(),
        partial(client.request, method, path, **kwargs),
    )
    return resp
