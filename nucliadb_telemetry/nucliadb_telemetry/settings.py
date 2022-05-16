from pydantic import BaseSettings


class TelemetrySettings(BaseSettings):
    jaeger_host: str = "jaeger.observability.svc.cluster.local"
    jaeger_port: int = 6831
    jeager_enabled: bool = False
    jaeger_http_port: int = 16686


telemetry_settings = TelemetrySettings()
