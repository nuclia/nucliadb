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
        logger.info("Configuring transaction queue with telemetry")
        jetstream = JetStreamContextTelemetry(
            jetstream, f"{service_name}_transaction", tracer_provider
        )
    return jetstream
