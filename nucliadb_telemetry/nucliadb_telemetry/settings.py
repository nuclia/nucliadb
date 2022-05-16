from pydantic import BaseSettings


class TelemetrySettings(BaseSettings):
    jaeger_host: str = "jaeger.observability.svc.cluster.local"
    jaeger_port: int = 6831
    jeager_enabled: bool = False


telemetry_settings = TelemetrySettings()
