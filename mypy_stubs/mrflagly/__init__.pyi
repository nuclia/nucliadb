from typing import Optional


class FlagService:
    def __init__(self, url: Optional[str] = None, data: Optional[str] = None) -> None:
        ...

    def enabled(
        self,
        flag_key: str,
        default: bool = False,
        context: Optional[dict] = None,
    ) -> bool:
        ...
