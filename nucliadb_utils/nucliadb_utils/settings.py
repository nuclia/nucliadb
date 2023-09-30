# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseSettings, Field
from pydantic.class_validators import root_validator


class RunningSettings(BaseSettings):
    debug: bool = True
    sentry_url: Optional[str] = None
    running_environment: str = Field(
        default="local", env=["environment", "running_environment"]
    )
    metrics_port: int = 3030
    metrics_host: str = "0.0.0.0"
    serving_port: int = 8080
    serving_host: str = "0.0.0.0"


running_settings = RunningSettings()


class HTTPSettings(BaseSettings):
    cors_origins: List[str] = ["http://localhost:4200"]


http_settings = HTTPSettings()


class FileBackendConfig(str, Enum):
    GCS = "gcs"
    S3 = "s3"
    PG = "pg"
    LOCAL = "local"
    NOT_SET = "notset"  # setting not provided

    @classmethod
    def _missing_(cls, value):
        """
        allow case insensitive enum values
        """
        for member in cls:
            if member.value == value.lower():
                return member


class StorageSettings(BaseSettings):
    file_backend: FileBackendConfig = Field(
        default=FileBackendConfig.NOT_SET, description="File backend storage type"
    )

    gcs_base64_creds: Optional[str] = Field(
        default=None,
        description="GCS JSON credentials of a service account encoded in Base64: https://cloud.google.com/iam/docs/service-account-overview",  # noqa
    )
    gcs_bucket: Optional[str] = Field(
        default=None,
        description="GCS Bucket name where files are stored: https://cloud.google.com/storage/docs/buckets",
    )
    gcs_location: Optional[str] = Field(
        default=None,
        description="GCS Bucket location: https://cloud.google.com/storage/docs/locations",
    )
    gcs_project: Optional[str] = Field(
        default=None,
        description="Google Cloud Project ID: https://cloud.google.com/resource-manager/docs/creating-managing-projects",  # noqa
    )
    gcs_bucket_labels: Dict[str, str] = Field(
        default={},
        description="Map of labels with which GCS buckets will be labeled with: https://cloud.google.com/storage/docs/tags-and-labels",  # noqa
    )
    gcs_endpoint_url: str = "https://www.googleapis.com"

    s3_client_id: Optional[str] = None
    s3_client_secret: Optional[str] = None
    s3_ssl: bool = True
    s3_verify_ssl: bool = True
    s3_max_pool_connections: int = 30
    s3_endpoint: Optional[str] = None
    s3_region_name: Optional[str] = None
    s3_bucket: Optional[str] = None

    local_files: Optional[str] = Field(
        default=None,
        description="If using LOCAL `file_backend` storage, directory where files should be stored",
    )
    upload_token_expiration: int = Field(
        default=3,
        description="Number of days that uploaded files are kept in Nulia's processing engine",
    )

    driver_pg_url: Optional[str] = None  # match same env var for k/v storage


storage_settings = StorageSettings()


class NucliaSettings(BaseSettings):
    nuclia_service_account: Optional[str] = None
    nuclia_public_url: str = "https://{zone}.nuclia.cloud"
    nuclia_cluster_url: str = "http://nucliadb_proxy.processing.svc.cluster.local:8080"
    nuclia_inner_predict_url: str = "http://predict.learning.svc.cluster.local:8080"

    nuclia_zone: str = "europe-1"
    onprem: bool = True

    nuclia_jwt_key: Optional[str] = None
    nuclia_hash_seed: int = 42
    nuclia_partitions: int = 1

    dummy_processing: bool = False
    dummy_predict: bool = False
    local_predict: bool = False
    local_predict_headers: Dict[str, str] = {}

    @root_validator(pre=True)
    def check_onprem_does_not_use_jwt_key(cls, values):
        if values.get("onprem") and values.get("jwt_key") is not None:
            raise ValueError("Invalid validation")
        return values


nuclia_settings = NucliaSettings()


class NucliaDBSettings(BaseSettings):
    nucliadb_ingest: Optional[str] = "ingest-orm-grpc.nucliadb.svc.cluster.local:8030"


nucliadb_settings = NucliaDBSettings()


class TransactionSettings(BaseSettings):
    transaction_jetstream_auth: Optional[str] = None
    transaction_jetstream_servers: List[str] = ["nats://localhost:4222"]
    transaction_local: bool = False


transaction_settings = TransactionSettings()


class IndexingSettings(BaseSettings):
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


class NATSConsumerSettings(BaseSettings):
    # Read about message ordering:
    #   https://docs.nats.io/nats-concepts/subject_mapping#when-is-deterministic-partitioning-needed
    nats_max_ack_pending: int = 1  # required for strict message ordering
    nats_max_deliver: int = 10000
    nats_ack_wait: int = 10 * 60
    nats_idle_heartbeat: float = 5.0


nats_consumer_settings = NATSConsumerSettings()
