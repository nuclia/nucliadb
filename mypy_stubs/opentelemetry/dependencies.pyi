from pkg_resources import Distribution as Distribution
from typing import Any, Collection, Optional

logger: Any

class DependencyConflict:
    required: str
    found: Optional[str]
    def __init__(self, required, found: Any | None = ...) -> None: ...

def get_dist_dependency_conflicts(dist: Distribution) -> Optional[DependencyConflict]: ...
def get_dependency_conflicts(deps: Collection[str]) -> Optional[DependencyConflict]: ...
