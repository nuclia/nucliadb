# NucliaDB Telemetry

Open telemetry compatible plugin to propagate traceid on FastAPI, Nats and GRPC with Asyncio.

ENV vars:

```
    JAEGER_ENABLED = True
    JAEGER_HOST = "127.0.0.1"
    JAEGER_PORT = server.port
```

On FastAPI you should add:

```python
    tracer_provider = get_telemetry("HTTP_SERVICE")
    app = FastAPI(title="Test API")  # type: ignore
    if not tracer_provider.initialized:
        await init_telemetry(tracer_provider)

    set_global_textmap(B3MultiFormat())
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)

    ..
    await init_telemetry(tracer_provider)  # To start asyncio task
    ..

```

On GRPC Server you should add:

```python
    tracer_provider = get_telemetry("GRPC_SERVER_SERVICE")
    telemetry_grpc = OpenTelemetryGRPC("GRPC_CLIENT_SERVICE", tracer_provider)
    if not tracer_provider.initialized:
        await init_telemetry(tracer_provider)

    set_global_textmap(B3MultiFormat())
    server = telemetry_grpc.init_server()
    helloworld_pb2_grpc.add_GreeterServicer_to_server(SERVICER, server)

    ..
    await init_telemetry(tracer_provider)  # To start asyncio task
    ..
```

On GRPC Client you should add:

```python
    tracer_provider = get_telemetry("GRPC_CLIENT_SERVICE")
    telemetry_grpc = OpenTelemetryGRPC("GRPC_CLIENT_SERVICE", tracer_provider)
    if not tracer_provider.initialized:
        await init_telemetry(tracer_provider)

    set_global_textmap(B3MultiFormat())
    channel = telemetry_grpc.init_client(f"localhost:{grpc_service}")
    stub = helloworld_pb2_grpc.GreeterStub(channel)

    ..
    await init_telemetry(tracer_provider)  # To start asyncio task
    ..

```

On Nats jetstream push subscriber you should add:

```python
    nc = await nats.connect(servers=[self.natsd])
    js = self.nc.jetstream()
    tracer_provider = get_telemetry("NATS_SERVICE")
    if not tracer_provider.initialized:
        await init_telemetry(tracer_provider)
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

On Nats publisher you should add:

```python
    nc = await nats.connect(servers=[self.natsd])
    js = self.nc.jetstream()
    tracer_provider = get_telemetry("NATS_SERVICE")
    if not tracer_provider.initialized:
        await init_telemetry(tracer_provider)

    set_global_textmap(B3MultiFormat())
    jsotel = JetStreamContextTelemetry(
        js, "NATS_SERVICE", tracer_provider
    )

     await jsotel.publish("testing.telemetry", request.name.encode())

```


On Nats jetstream pull subscription you can use different patterns if you want to
just get one message and exit or pull several ones. For just one message

```python
    nc = await nats.connect(servers=[self.natsd])
    js = self.nc.jetstream()
    tracer_provider = get_telemetry("NATS_SERVICE")
    if not tracer_provider.initialized:
        await init_telemetry(tracer_provider)
    set_global_textmap(B3MultiFormat())
    jsotel = JetStreamContextTelemetry(
        js, "NATS_SERVICE", tracer_provider
    )

    # You can use either pull_subscribe or pull_subscribe_bind
    subscription = await jsotel.pull_subscribe(
        subject="testing.telemetry",
        durable="consumer_name"
        stream="testing",
    )

    async def callback(message):
        # Do something with your message
        # and optionally return something
        return True

    try:
        result = await jsotel.pull_one(subscription, callback)
    except errors.TimeoutError
        pass

```
For multiple messages just wrap it in a loop:

```python
    while True:
        try:
            result = await jsotel.pull_one(subscription, callback)
        except errors.TimeoutError
            pass

```


On Nats client (NO Jestream! ) publisher you should add:

```python
    nc = await nats.connect(servers=[self.natsd])
    js = self.nc.jetstream()
    tracer_provider = get_telemetry("NATS_SERVICE")
    if not tracer_provider.initialized:
        await init_telemetry(tracer_provider)

    set_global_textmap(B3MultiFormat())
    ncotel = NatsClientTelemetry(
        nc, "NATS_SERVICE", tracer_provider
    )

     await ncotel.publish("testing.telemetry", request.name.encode())

```

On Nats client (NO Jestream! ) subscriber you should add:

```python
    nc = await nats.connect(servers=[self.natsd])
    js = self.nc.jetstream()
    tracer_provider = get_telemetry("NATS_SERVICE")
    if not tracer_provider.initialized:
        await init_telemetry(tracer_provider)
    set_global_textmap(B3MultiFormat())
    ncotel = NatsClientContextTelemetry(
        js, "NATS_SERVICE", tracer_provider
    )

    subscription = await ncotel.subscribe(
        subject="testing.telemetry",
        queue="queue_nname",
        cb=handler,
    )

```


On Nats client (NO Jestream! ) request you should add:

```python
    nc = await nats.connect(servers=[self.natsd])
    js = self.nc.jetstream()
    tracer_provider = get_telemetry("NATS_SERVICE")
    if not tracer_provider.initialized:
        await init_telemetry(tracer_provider)

    set_global_textmap(B3MultiFormat())
    ncotel = NatsClientTelemetry(
        nc, "NATS_SERVICE", tracer_provider
    )

    response = await ncotel.request("testing.telemetry", request.name.encode())

```

And to handle responses on the other side, you can use the same pattern as in plain Nats client
subscriber, just adding the `msg.respond()` on the handler when done
