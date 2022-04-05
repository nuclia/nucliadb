# How to develop on nucliaDB

You can develop by test driven using the dependency fixtures or by deploying localy the required dependencies.

## Running dependecy layers localy

First we need the underlying layers: broker, storage, keyvalue

### Minimal set

In order to start the dependency layers use docker compose:

    docker-compose -f docker-compose-deps.yml start

### Large set

In order to start the dependency layers with scalable components use docker compose:

    docker-compose -f docker-compose-deps.yml start

It will start 3 Nodes of Nats, 3 Nodes of TiKV and 4 Nodes of Minio

## Running ingest

    docker-compose -f docker-compose-deps.yaml -f docker-compose-distributed.yaml build ingest
    docker-compose -f docker-compose-deps.yaml -f docker-compose-distributed.yaml run --service-ports --rm ingest

### Testing ingest

    tiup playground --mode tikv-slim --without-monitor

### Connecto to ingest aiomonitor

    rlwrap nc localhost 50101

## Running writer

    docker-compose -f docker-compose-deps.yaml -f docker-compose-distributed.yaml build writer
    docker-compose -f docker-compose-deps.yaml -f docker-compose-distributed.yaml run --service-ports --rm writer

## Running node

    docker-compose -f docker-compose-deps.yaml -f docker-compose-distributed.yaml build node1
    docker-compose -f docker-compose-deps.yaml -f docker-compose-distributed.yaml run --service-ports --rm node1
