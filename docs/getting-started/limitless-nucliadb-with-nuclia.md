# Limitless NucliaDB with Nuclia

NucliaDB is a perfect place to store embeddings (vectors) extracted
from your information and search for it. But information extraction is
not an easy task. That's why we propose you to try Nuclia, our
processing service fully integrated with NucliaDB. With it, you can
send to process **any** kind of file (text, audio, images, videos,
links...), sit and enjoy seeing how relevant extracted information
come back and is indexed in your NucliaDB. And anything is saved on
the cloud!

Furthemore, semantic search will become child's play! Your queries
will be converted to vectors automatically and semantic search will
work out of the box!

So, what are you waiting for? [Sign up for a free
account](https://nuclia.cloud/user/signup), follow this tutorial and
judge for yourself!

## Get a NucliaDB token to connect to Nuclia Understanding API™

First of all, you need a _Nuclia Understanding API™_ key (NUA key from
now) that you can get following these tutorials:

- [Create your free Nuclia account](https://docs.nuclia.dev/docs/quick-start/create)
- [Get a NUA key](https://docs.nuclia.dev/docs/understanding/intro#get-a-nua-key)

## Start NucliaDB

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

## Create a Knowledge Box container

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

To facilitate code examples, please, export these variables with your
current values:
```bash
export KB_UUID=<YOUR-KB-UUID>
```

## Upload a file

After starting NucliaDB and creating a Knowledge Box you can upload a
file:

```bash
curl "http://localhost:8080/api/v1/kb/${KB_UUID}/upload" \
  -X POST \
  -H "X-NUCLIADB-ROLES: WRITER" \
  -H "X-FILENAME: `echo -n "myfile" | base64`"
  -T /path/to/file
```

The file will be sent to process on Nuclia. When it finishes
processing, your local NucliaDB will receive a message with the
extracted information.

## Search

Once we finish uploading files and getting the processed results, we
can do searches on our data. We can even do the same exact searches as
before, but having now a NUA key, gives us access to semantic search
out of the box.

```bash
curl http://localhost:8080/api/v1/kb/${KB_UUID}/search?query=your+own+query \
  -H "X-NUCLIADB-ROLES: READER" \
```

This search will transform the query in a vector and perform a
semantic search on our data as well as the regular fuzzy search. Easy,
right?
