# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging

from grpc import ChannelCredentials, aio

from nucliadb_telemetry.grpc import GRPCTelemetry
from nucliadb_telemetry.grpc_sentry import SentryInterceptor
from nucliadb_telemetry.utils import get_telemetry

logger = logging.getLogger(__name__)

RETRY_OPTIONS = [
    (
        "grpc.service_config",
        json.dumps(
            {
                "name": [{}],  # require to enable retrying all methods
                "retryPolicy": {
                    "maxAttempts": 4,
                    "initialBackoff": "0.02s",
                    "maxBackoff": "2s",
                    "backoffMultiplier": 2,
                    "retryableStatusCodes": [
                        "UNAVAILABLE",
                        "DEADLINE_EXCEEDED",
                        "ABORTED",
                        "CANCELLED",
                    ],
                },
                "waitForReady": True,
            }
        ),
    ),
    ("grpc.max_metadata_size", 1 * 1024 * 1024),
]


def get_traced_grpc_channel(
    address: str,
    service_name: str,
    credentials: ChannelCredentials | None = None,
    variant: str = "",
    max_send_message: int = 100,
) -> aio.Channel:
    tracer_provider = get_telemetry(service_name)
    if tracer_provider is not None:  # pragma: no cover
        telemetry_grpc = GRPCTelemetry(service_name + variant, tracer_provider)
        channel = telemetry_grpc.init_client(
            address,
            max_send_message=max_send_message,
            credentials=credentials,
            options=RETRY_OPTIONS,
        )
    else:
        options = [
            ("grpc.max_receive_message_length", max_send_message * 1024 * 1024),
            ("grpc.max_send_message_length", max_send_message * 1024 * 1024),
            *RETRY_OPTIONS,
        ]
        channel = aio.insecure_channel(address, options=options)
    return channel


def get_traced_grpc_server(service_name: str, max_receive_message: int = 100):
    tracer_provider = get_telemetry(service_name)
    if tracer_provider is not None:  # pragma: no cover
        otgrpc = GRPCTelemetry(f"{service_name}_grpc", tracer_provider)

        server = otgrpc.init_server(
            max_receive_message=max_receive_message,
            interceptors=[SentryInterceptor()],
        )
    else:
        options = [("grpc.max_receive_message_length", max_receive_message * 1024 * 1024)]
        server = aio.server(options=options)
    return server
