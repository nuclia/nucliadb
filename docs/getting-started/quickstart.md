# Quickstart

Getting started with NucliaDB is as easy as running a docker
image. Although NucliaDB is a distributed database, we have a
containerized standalone version where all it's started in one node.

**Note:** we use own concepts like Knowledge Box that you may not
understand. Please, refer to [Basic Concepts](basic-concepts.md) or
ask in the community if anything is unclear.

## NucliaDB: run, index and search!

### Start NucliaDB minimal

Let's start running the latest NucliaDB docker image.

```bash
docker run -it \
       -e LOG=INFO \
       -p 8080:8080 \
       -p 8060:8060 \
       -p 8040:8040 \
       -v nucliadb-standalone:/data \
       nuclia/nucliadb:latest
```

If you want to learn other ways to run NucliaDB, please refer to
[installation tutorial](installation.md).

### Create a Knowledge Box

```bash
curl 'http://localhost:8080/api/v1/kbs' \
    -X POST \
    -H "X-NUCLIADB-ROLES: MANAGER" \
    -H "Content-Type: application/json" \
    --data-raw '{
  "slug": "mykb",
  "title": "My KB"
}'
```

This call will return you a Knowledge Box UUID nedded for further API
calls.

To facilitate code examples, please, export these variables with your
current values:
```bash
export KB_SLUG=<YOUR-KB-SLUG>
export KB_UUID=<YOUR-KB-UUID>
```

### Manually uploading some data

NucliaDB has a simple Python client which provides an easy way to
import and export content. We have some examples
[here](../../nucliadb_client/examples/).


The [simple.py](../../nucliadb_client/examples/simple.py) shows how to
extract vectors from text and to insert them all inside a
NucliaDB. Using a running NucliaDB, download the script and test it
with:

```bash
pip install nucliadb_client
python simple.py \
       --host=localhost \
       --http=8080 \
       --grpc=8060 \
       --train=8040 \
       --kb ${KB_SLUG}
```

This will upload text and vectors to the specified Knowledge Box. 

### Searching knowledge

#### Text search

Once we have knowledge inside our running NucliaDB you have it, it's
time to search. Uploaded texts can be found
[here](../../nucliadb_client/examples/articles.json) and are parts of
Wikipedia articles.

Let's start with a simple query:

```bash
curl "http://localhost:8080/api/v1/kb/${KB_UUID}/search?query=meaning" \
     -H "X-NUCLIADB-ROLES: READER"
```

It will return the resources where the text is found and also the
paragraphs in that resource where the queries word is found.

A fuzzy search with 1 Levenshtein distance is performed, so we can
make a typo and keep seeing results:

```bash
curl "http://localhost:8080/api/v1/kb/${KB_UUID}/search?query=fault+t0lerance" \
     -H "X-NUCLIADB-ROLES: READER"
```

#### Semantic search

Text search is fine, but not rocket science anymore. We are a semantic
search engine on unstructured data, so let's demonstrate it.

To perform a semantic search, we need two things: to extract and save
vectors from the stored knowledge and to pass a similar or exact
vector to the search endpoint.

As we have inserted manually (using a Python script) the vectors, we
can search semantically using a POST request and passing an exact or a
similar vector:

```bash
VECTOR=$(python simple.py --host=localhost --http=8080 --grpc=8060 --train=8040 --kb ${KB_SLUG} --print-random-vector) \
&& curl "http://localhost:8080/api/v1/kb/${KB_UUID}/search" \
     -X POST \
     -H "X-NUCLIADB-ROLES: READER" \
     --data-raw "{
  \"query\": \"whatever\",
  \"vector\": ${VECTOR}
}"
```


## Unleash NucliaDB true power with Nuclia

Although it's not hard to insert your text and extracted vectors using
the NucliaDB client, achieving a good information extraction from a ML
pipeline and process all your documents it's definitely tough.

That's why we can join strenghts with Nuclia and make this task
astonishingly easy.

Keep reading [here](./limitless-nucliadb-with-nuclia.md) to learn how to connect NucliaDB with Nuclia API.
