from typing import List, Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    cache_memory_size: int = 209715200
    cache_pubsub_channel: str = "nucliadb.cache.invalidations"
    cache_pubsub_redis_url: Optional[str] = None
    cache_pubsub_nats_url: List[str] = ["localhost:4222"]
    cache_pubsub_nats_auth: Optional[str] = None
    cache_pubsub_driver: str = "nats"  # redis | nats


settings = Settings()
