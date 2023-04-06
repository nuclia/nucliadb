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

import logging
from typing import Optional

from grpc import aio  # type: ignore
from grpc import ChannelCredentials
from nucliadb_telemetry.grpc import GRPCTelemetry
from nucliadb_telemetry.utils import get_telemetry

logger = logging.getLogger(__name__)


def get_traced_grpc_channel(
    address: str,
    service_name: str,
    credentials: Optional[ChannelCredentials] = None,
    variant: str = "",
    max_send_message: int = 100,
) -> aio.Channel:
    tracer_provider = get_telemetry(service_name)
    if tracer_provider is not None:  # pragma: no cover
        telemetry_grpc = GRPCTelemetry(service_name + variant, tracer_provider)
        channel = telemetry_grpc.init_client(
            address, max_send_message=max_send_message, credentials=credentials
        )
    else:
        channel = aio.insecure_channel(address)
    return channel


def get_traced_grpc_server(service_name: str, max_receive_message: int = 100):
    tracer_provider = get_telemetry(service_name)
    if tracer_provider is not None:  # pragma: no cover
        otgrpc = GRPCTelemetry(f"{service_name}_grpc", tracer_provider)
        server = otgrpc.init_server(max_receive_message=max_receive_message)
    else:
        options = [
            ("grpc.max_receive_message_length", max_receive_message * 1024 * 1024),
        ]
        server = aio.server(options=options)
    return server
