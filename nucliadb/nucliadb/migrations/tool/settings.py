from typing import Optional

import pydantic


class Settings(pydantic.BaseSettings):
    redis_url: Optional[str] = None
