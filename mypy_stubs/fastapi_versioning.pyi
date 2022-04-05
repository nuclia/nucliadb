from typing import Any, Callable, TypeVar

CallableT = TypeVar("CallableT", bound=Callable[..., Any])

def version(major: int, minor: int = 0) -> Callable[[CallableT], CallableT]: ...

class VersionedFastAPI:
    routes: Any
    router: Any
    def __init__(
        self,
        base_app: Any,
        version_format: str,
        prefix_format: str,
        routes: Any,
        **kwargs
    ): ...
    def add_route(self, url: str, func: Any): ...
