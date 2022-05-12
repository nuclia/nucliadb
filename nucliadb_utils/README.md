# nucliadb util python library

- Nats driver

## Telemetry

ENV vars:

```
    JEAGER_ENABLED = True
    JAEGER_HOST = "127.0.0.1"
    JAEGER_PORT = server.port
```

On FastAPI you should add:

```python
    tracer_provider = init_telemetry("HTTP_SERVICE")
    app = FastAPI(title="Test API")  # type: ignore
    set_global_textmap(B3MultiFormat())
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)
```

On GRPC Server you should add:

```python
    tracer_provider = init_telemetry("GRPC_SERVER_SERVICE")
    telemetry_grpc = OpenTelemetryGRPC("GRPC_CLIENT_SERVICE", tracer_provider)
    set_global_textmap(B3MultiFormat())
    server = telemetry_grpc.init_server()
    helloworld_pb2_grpc.add_GreeterServicer_to_server(SERVICER, server)

```

On GRPC Client you should add:

```python
    tracer_provider = init_telemetry("GRPC_CLIENT_SERVICE")
    telemetry_grpc = OpenTelemetryGRPC("GRPC_CLIENT_SERVICE", tracer_provider)
    set_global_textmap(B3MultiFormat())
    channel = telemetry_grpc.init_client(f"localhost:{grpc_service}")
    stub = helloworld_pb2_grpc.GreeterStub(channel)

```

On Nats Server you should add:

```python
    nc = await nats.connect(servers=[self.natsd])
    js = self.nc.jetstream()
    tracer_provider = init_telemetry("NATS_SERVICE")
    set_global_textmap(B3MultiFormat())
    jsotel = JetStreamContextTelemetry(
        js, "NATS_SERVICE", tracer_provider
    )

    subscription = await jsotel.subscribe(
        subject="testing.telemetry",
        stream="testing",
        cb=handler,
    )

```

On Nats client you should add:

```python
    nc = await nats.connect(servers=[self.natsd])
    js = self.nc.jetstream()
    tracer_provider = init_telemetry("NATS_SERVICE")
    set_global_textmap(B3MultiFormat())
    jsotel = JetStreamContextTelemetry(
        js, "NATS_SERVICE", tracer_provider
    )

     await jsotel.publish("testing.telemetry", request.name.encode())

```
