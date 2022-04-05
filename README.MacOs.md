# MacOS Silicon

In order to run some options on the repo on MacOS Silicon chip you may need extra steps.

## Run nucliadb_search tests

In order to run `nucliadb_search` and `nucliadb_one` tests you need to have an arm compiled version of nucliadb_node so you need to compute on your local machine:

`docker build -t eu.gcr.io/stashify-218417/node:latest . -f Dockerfile.node`
