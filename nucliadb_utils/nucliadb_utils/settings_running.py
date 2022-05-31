from typing import Optional

from pydantic import BaseSettings


class RunningSettings(BaseSettings):
    debug: bool = True
    sentry_url: Optional[str] = None
    running_environment: str = "local"
    logging_integration: bool = False
    log_level: str = "DEBUG"
    activity_log_level: str = "INFO"
    swim_level: str = "INFO"
    metrics_port: int = 3030
    metrics_host: str = "0.0.0.0"
    serving_port: int = 8080
    serving_host: str = "0.0.0.0"


running_settings = RunningSettings()
