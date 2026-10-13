import httpx


class MarkLogicError(Exception):
    pass


class MarkLogicResponseError(MarkLogicError):
    def __init__(self, message: str, *, response: httpx.Response | None = None, code: str | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code if response is not None else None
        self.code = code


class MarkLogicProtocolError(MarkLogicError):
    """The server response does not match the expected REST protocol."""


class NoHealthyUpstreamError(MarkLogicError):
    """Raised when MarkLogic returns 503 'no healthy upstream'.

    This is a transient load-balancer error that can be safely retried.
    """

    pass


class DatabaseDoesNotExist(MarkLogicResponseError):
    """Raised when MarkLogic reports that the target database no longer exists."""

    pass


class RoleDoesNotExist(MarkLogicError):
    """Raised when MarkLogic reports SEC-ROLEDNE: a referenced role no longer exists.

    This commonly happens when a document write races with project deletion: project
    roles (e.g. ``project-{id}-reader``/``-writer``) are deleted as the final step of
    project cleanup, so a write that started earlier (e.g. a long-running background
    pipeline) can still be in flight and reference those roles by the time it finishes.

    Can also happen when a role is created via one host and immediately referenced
    in a subsequent call to another host, who may not yet be on the same server timestamp
    as the first host.
    """

    pass
