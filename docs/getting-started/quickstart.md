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

### Manually uploading some data

NucliaDB has a simple Python client which provides an easy way to
import and export content. We have some examples
[here](../../nucliadb_client/examples/).

The [simple.py](../../nucliadb_client/examples/simple.py) shows how to
extract vectors from text and to insert them all inside a
NucliaDB. Using a running NucliaDB, we can test it with:

```bash
cd nucliadb_client/examples
python simple.py \
       --host=localhost \
       --http=8080 \
       --grpc=8060 \
       --train=8040 \
       --kb <YOUR-KB-SLUG>
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
curl 'http://localhost:8080/api/v1/kb/<YOUR-KB-UUID>/search?query=meaning\
     -X GET \
     -H "X-NUCLIADB-ROLES: READER"
```

It will return the resources where the text is found and also the
paragraphs in that resource where the queries word is found.

A fuzzy search with 1 Levenshtein distance is performed, so we can
make a typo and keep seeing results:

```bash
curl 'http://localhost:8080/api/v1/kb/<YOUR-KB-UUID>/search?query=fault+t0lerance\
     -X GET \
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
cd nucliadb_client/examples
VECTOR=$(python simple.py --host=localhost --http=8080 --grpc=8060 --train=8040 --kb simple --print-random-vector) \
&& curl 'http://localhost:8080/api/v1/kb/<YOUR-KB-UUID>/search' \
     -X POST \
     -H "X-NUCLIADB-ROLES: READER" \
     --data-raw "{
  \"query\": \"whatever\",
  \"vector\": $(python simple.py --host=localhost --http=8080 --grpc=8060 --train=8040 --kb simple --print-random-vector)
}"
```


## Boundless NucliaDB with Nuclia

Although it's not hard to insert your text and extracted vectors using
the NucliaDB client, achieving a good information extraction from a ML
pipeline and process all your documents it's definitely tough.

That's why we can join strenghts with Nuclia and make this task
astonishingly easy.

### Get a NucliaDB token to connect to Nuclia Understanding API™
 
First of all, you need a _Nuclia Understanding API™_ key (NUA key from
now) that you can get following these tutorials:

- [Create your free Nuclia account](https://docs.nuclia.dev/docs/quick-start/create)
- [Get a NUA key](https://docs.nuclia.dev/docs/understanding/intro#get-a-nua-key)

### Start NucliaDB

Once you have your NUA key, we will start NucliaDB with it

```bash
docker run -it \
       -e LOG=INFO \
       -e NUA_API_KEY=<YOUR-NUA-API-KEY> \
       -p 8080:8080 \
       -p 8060:8060 \
       -p 8040:8040 \
       -v nucliadb-standalone:/data \
       nuclia/nucliadb:latest
```

### Create a Knowledge Box container

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

This call will return you a Knowledge Box UUID nedded for further API calls.

### Upload a file

After starting NucliaDB and creating a Knowledge Box you can upload a file:

```bash
curl http://localhost:8080/api/v1/kb/<your-knowledge-box-id>/upload \
  -X POST \
  -H "X-NUCLIADB-ROLES: WRITER" \
  -H "X-FILENAME: `echo -n "myfile" | base64`"
  -T /path/to/file
```

The file will be sent to process on Nuclia. When it finishes
processing, your local NucliaDB will receive a message with the
extracted information. 

### Search

Once we finish uploading files and getting the processed results, we
can do searches on our data. We can even do the same exact searches as
before, but having now a NUA key, gives us access to semantic search
out of the box.

```bash
curl http://localhost:8080/api/v1/kb/<your-knowledge-box-id>/search?query=your+own+query \
  -X GET \
  -H "X-NUCLIADB-ROLES: READER" \
```

This search will transform the query in a vector and perform a
semantic search on our data as well as the regular fuzzy search. Easy,
right?
