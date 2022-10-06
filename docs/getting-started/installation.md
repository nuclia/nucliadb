# NucliaDB installation

NucliaDB can be installed and deployed in three different ways:
standalone, with Redis or fully distributed.

## NucliaDB Standalone

This is the easiest way to start nucliadb. It's ideal for testing but
**don't use it in production environments!**

NucliaDB standalone uses a local driver, that means all your data will
be saved on local disk.

It can be run with Python or using our docker version.

### Docker

Run the latest NucliaDB docker image with:

```bash
docker run -it \
       -e LOG=INFO \
       -p 8080:8080 \
       -p 8060:8060 \
       -p 8040:8040 \
       -v nucliadb-standalone:/data \
       nuclia/nucliadb:latest
```

It will open the following ports:

- HTTP on 8080
- gRPC on 8060
- ML Train on 8040

### Python

Create a virtual environment and install nucliadb with `pip`:

```bash
pip install nucliadb
```

Run NucliaDB with a local driver using:

```bash
nucliadb \
    --driver=LOCAL \
    --maindb=data/maindb \
    --blob=data/blob \
    --node=data/node \
    --zone=europe-1 \
    --http 8080 \
    --grpc 8060 \
    --train 8040 \
    --log=INFO
```


## NucliaDB Standalone with Redis

NucliaDB with Redis is a more powerful combination. Although this
option uses the NucliaDB standalone version and won't scale well, the
use of Readis driver allow NucliaDB to work in a transactional fashion

Even you can run it with Python as before using the Redis driver and
setting the maindb to the appropiate Redis URL, we will show a simpler
deployment with `docker-compose`.

### Docker compose

```bash
docker-compose -f docker-compose.yaml up
```

It will start a NucliaDB standalone with Redis driver along with a
Redis container. Exposed ports are the same as before.


## NucliaDB distributed

Distributed deployment is not documented yet. If you have any doubt,
please contact us!
