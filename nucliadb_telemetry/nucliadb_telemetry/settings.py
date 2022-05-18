from pydantic import BaseSettings


class TelemetrySettings(BaseSettings):
    jaeger_agent_host: str = "localhost"
    jaeger_agent_port: int = 6831
    jaeger_enabled: bool = False
    jaeger_query_port: int = 16686
    jaeger_query_host: str = "jaeger.observability.svc.cluster.local"


telemetry_settings = TelemetrySettings()
