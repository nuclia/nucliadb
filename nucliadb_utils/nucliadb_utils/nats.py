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

from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext
from nucliadb_telemetry.jetstream import JetStreamContextTelemetry
from nucliadb_telemetry.utils import get_telemetry

logger = logging.getLogger(__name__)


def get_traced_jetstream(nc: NATS, service_name: str) -> JetStreamContext:
    jetstream = nc.jetstream()
    tracer_provider = get_telemetry(service_name)

    if tracer_provider is not None and jetstream is not None:  # pragma: no cover
        logger.info(f"Configuring {service_name} jetstream with telemetry")
        jetstream = JetStreamContextTelemetry(jetstream, service_name, tracer_provider)
    return jetstream
