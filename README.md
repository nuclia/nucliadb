![CI](https://github.com/stashify/nucliadb/actions/workflows/nucliadb_one.yml/badge.svg)
[![codecov](https://codecov.io/gh/nuclia/nucliadb/branch/main/graph/badge.svg?token=06SRGAV5SS)](https://codecov.io/gh/nuclia/nucliadb)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.0-4baaaa.svg)](CODE_OF_CONDUCT.md)
[![License: AGPL V3](https://img.shields.io/badge/license-AGPL%20V3-blue)](LICENCE.md)
![Twitter Follow](https://img.shields.io/twitter/follow/nuclia_?color=%231DA1F2&logo=Twitter&style=plastic)
![Discord](https://img.shields.io/discord/911636727150575649?logo=Discord&logoColor=%23FFFFFF&style=plastic)
![Rust](https://img.shields.io/badge/Rust-black?logo=rust&style=plastic)
![Python](https://img.shields.io/badge/Python-black?logo=python&style=plastic)

<p align="center">
  <img src="docs/assets/images/nuclia_db_positiu.svg" alt="Nuclia" height="100">
</p>
<h3 align="center">NLP Database</h3>

<h4 align="center">
  <a href="docs/getting-started/quickstart">Quickstart</a> |
  <a href="docs/">Docs</a> |
  <a href="docs/tutorials">Tutorials</a> |
  <a href="https://discord.gg/W6RKm2Vnhq">Chat</a>
</h4>

### Check out our [blog post](https://nuclia.com/blog/first-release/) to grasp what we have been doing for the last months.

NucliaDB is a distributed search engine built from the ground up to offer high accuracy and semantic search on unstructured data. By mere mortals for mere mortals, NucliaDB's architecture is as simple as possible to be scalable and deliver what a NLP Database requires.

NucliaDB is written in Rust and Python and built on top of the mighty [tantivy](https://github.com/quickwit-oss/tantivy) library. We designed it to index big datasets and provide multi-teanant suport.

# Features

- Store original data, extracted and understanding data on object and blob storage
- Index fields, paragraphs, semantic sentences on index storage
- Cloud extraction and understanding with Nuclia Understanding API™
- Cloud connection to train ML models with Nuclia Learning API™
- Container security based with Reader, Manager, Writer Roles
- Resources with multiple fields and metadata
- Text/HTML/Markdown plain fields support
- File fields support with direct upload and TUS upload
- Link fields support
- Conversation fields support
- Blocks/Layout field support
- Eventual consistency transactions based on Nats.io
- Distributed source of truth with TiKV and Redis support
- Blob support with S3-compatible API and GCS
- Replication of index storage
- Distributed search
- Cloud-native: Kubernetes only

### Upcomming Features

- Blob support with Azure Blob storage
- Index relations on index storage

## Achitecture

<p align="center">
  <img src="docs/assets/images/arquitecture.svg" alt="Architecture" width="500px" style="background-color: #fff">
</p>

## Quickstart

### Get a NucliaDB token to connect to Nuclia Understanding API™

Only needed if you want to use _Nuclia Understanding API™_ and _Nuclia Learning API™_

### Start NucliaDB minimal

First we need object storage and blob storage

```
docker run redis
docker run minio
```

```
pip install nucliadb
nucliadb
```

### Create a Knowledge box container

```bash
curl http://localhost:8080/v1/kb \
  -X POST \
  -H "X-NUCLIADB-ROLE: MANAGER" \
```

### Upload a file

After starting NucliaDB and creating a Knowledge Box you can upload a file:

```bash
curl http://localhost:8080/v1/kb/<your-knowledge-box-id>/upload \
  -X POST \
  -H "X-NUCLIADB-ROLE: WRITER" \
  -T /path/to/file
```

### Search a file

After starting NucliaDB and creating a Knowledge Box you can upload a file:

```bash
curl http://localhost:8080/v1/kb/<your-knowledge-box-id>/search \
  -X GET \
  -H "X-NUCLIADB-ROLE: READER" \
```

## API Tutorials

- [Upload a file](https://docs.nuclia.dev/docs/quick-start/push)

## Reference

- [Nuclia](https://docs.nuclia.dev/)
- [API Reference](https://docs.nuclia.dev/docs/api)
- [NucliaDB internal documentation](docs/internal/)

## Meta

- [Code of conduct](CODE_OF_CONDUCT.md)
- [Contributing](CONTRIBUTING.md)
