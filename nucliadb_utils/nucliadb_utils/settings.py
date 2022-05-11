from typing import Dict, List, Optional

from pydantic import BaseSettings
from pydantic.class_validators import root_validator


class RunningSettings(BaseSettings):
    debug: bool = True
    sentry_url: Optional[str] = None
    running_environment: str = "local"
    logging_integration: bool = False
    log_level: str = "DEBUG"
    activity_log_level: str = "INFO"
    swim_level: str = "INFO"


running_settings = RunningSettings()


class HTTPSettings(BaseSettings):
    cors_origins: List[str] = ["http://localhost:4200"]


http_settings = HTTPSettings()


class StorageSettings(BaseSettings):
    file_backend: str = "gcs"  # gcs | s3

    gcs_base64_creds: Optional[str] = None
    gcs_bucket: Optional[str] = None
    gcs_location: Optional[str] = None
    gcs_project: Optional[str] = None
    gcs_bucket_labels: Dict[str, str] = {}
    gcs_endpoint_url: str = "https://www.googleapis.com"

    s3_client_id: Optional[str] = None
    s3_client_secret: Optional[str] = None
    s3_ssl: bool = True
    s3_verify_ssl: bool = True
    s3_max_pool_connections: int = 30
    s3_endpoint: Optional[str] = None
    s3_region_name: Optional[str] = None
    s3_bucket: Optional[str] = None

    local_files: Optional[str] = None


storage_settings = StorageSettings()


class NucliaSettings(BaseSettings):
    nuclia_service_account: Optional[str] = None
    nuclia_public_url: Optional[str] = "https://{zone}.nuclia.cloud"
    nuclia_cluster_url: Optional[
        str
    ] = "http://nucliadb_proxy.processing.svc.cluster.local:8080"
    nuclia_inner_predict_url: Optional[
        str
    ] = "http://predict.learning.svc.cluster.local:8080"

    nuclia_zone: Optional[str] = "dev"
    onprem: bool = True

    nuclia_jwt_key: Optional[str] = None
    nuclia_hash_seed: int = 42
    nuclia_partitions: int = 1

    dummy_processing: bool = False

    @root_validator(pre=True)
    def check_relation_is_valid(cls, values):
        if values.get("onprem") and values.get("jwt_key") is not None:
            raise ValueError("Invalid validation")
        return values


nuclia_settings = NucliaSettings()


class NucliaDBSettings(BaseSettings):
    nucliadb_ingest: Optional[str] = "ingest.nucliadb.svc.cluster.local:4242"


nucliadb_settings = NucliaDBSettings()


class TransactionSettings(BaseSettings):
    transaction_jetstream_auth: Optional[str] = None
    transaction_jetstream_servers: Optional[List[str]] = ["nats://localhost:4222"]
    transaction_jetstream_target: Optional[str] = "ndb.consumer.{partition}"
    transaction_jetstream_group: Optional[str] = "nucliadb-{partition}"
    transaction_jetstream_stream: Optional[str] = "nucliadb"
    transaction_local: bool = False


transaction_settings = TransactionSettings()


class IndexingSettings(BaseSettings):

    index_jetstream_target: Optional[str] = "node.{node}"
    index_jetstream_group: Optional[str] = "node-{node}"
    index_jetstream_stream: Optional[str] = "node"
    index_jetstream_servers: List[str] = []
    index_jetstream_auth: Optional[str] = None
    index_local: bool = False


indexing_settings = IndexingSettings()


class AuditSettings(BaseSettings):
    audit_driver: str = "basic"
    audit_jetstream_target: Optional[str] = "audit.{partition}.{type}"
    audit_jetstream_servers: List[str] = []
    audit_jetstream_auth: Optional[str] = None
    audit_partitions: int = 3
    audit_stream: str = "audit"
    audit_hash_seed: int = 1234


audit_settings = AuditSettings()


class TelemetrySettings(BaseSettings):
    jaeger_host: str = "jaeger.observability.svc.cluster.local"
    jaeger_port: int = 6831
    jeager_enabled: bool = False


telemetry_settings = TelemetrySettings()
