from typing import Any, Awaitable, Callable

from nucliadb.common.context import ApplicationContext

TaskCallback = Callable[[ApplicationContext, Any], Awaitable[None]]
