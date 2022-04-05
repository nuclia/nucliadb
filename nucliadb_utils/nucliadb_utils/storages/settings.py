import os
from typing import Dict, Optional

from pydantic import BaseSettings


class Settings(BaseSettings):

    gcs_deadletter_bucket: Optional[str] = None
    gcs_indexing_bucket: Optional[str] = None

    gcs_threads: int = 3
    gcs_labels: Dict[str, str] = {}

    s3_deadletter_bucket: Optional[str] = None
    s3_indexing_bucket: Optional[str] = None

    local_testing_files: str = os.path.dirname(__file__)


settings = Settings()
