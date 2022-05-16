from opentelemetry.sdk.resources import SERVICE_NAME  # type: ignore
from opentelemetry.sdk.resources import Resource  # type: ignore
from opentelemetry.sdk.trace import TracerProvider  # type: ignore

from nucliadb_telemetry.batch_span import BatchSpanProcessor
from nucliadb_telemetry.jaeger import JaegerExporterAsync
from nucliadb_telemetry.settings import telemetry_settings

GLOBAL_PROVIDER = {}


def get_telemetry(service_name: str):
    if service_name not in GLOBAL_PROVIDER and service_name:
        GLOBAL_PROVIDER[service_name] = init_telemetry(service_name)
    return GLOBAL_PROVIDER.get(service_name)


def init_telemetry(service_name: str):
    if telemetry_settings.jeager_enabled is False:
        return

    tracer_provider = TracerProvider(
        resource=Resource.create({SERVICE_NAME: service_name})
    )

    # create a JaegerExporter
    jaeger_exporter = JaegerExporterAsync(
        # configure agent
        agent_host_name=telemetry_settings.jaeger_host,
        agent_port=telemetry_settings.jaeger_port,
        # optional: configure also collector
        # collector_endpoint='http://localhost:14268/api/traces?format=jaeger.thrift',
        # username=xxxx, # optional
        # password=xxxx, # optional
        # max_tag_value_length=None # optional
    )

    # Create a BatchSpanProcessor and add the exporter to it
    span_processor = BatchSpanProcessor(jaeger_exporter)

    # add to the tracer
    tracer_provider.add_span_processor(span_processor)
    return tracer_provider
